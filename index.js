'use strict';

const crypto = require('crypto');
const fs = require('fs');
const util = require('util');
const path = require('path');
const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const unlink = util.promisify(fs.unlink);
const mkdir = util.promisify(fs.mkdir);
const defaultConfig = require('./defaults/config');
const packageJSON = require('./package.json');

const DEFAULT_MODULE_ALIAS = 'airdropper';

module.exports = class Airdropper {
  constructor({alias, config, appConfig, logger}) {
    this.options = {...defaultConfig, ...config};
    this.appConfig = appConfig;
    this.alias = alias || DEFAULT_MODULE_ALIAS;
    this.logger = logger;
    this.multisigWalletInfo = {};
    this.pendingTransfers = new Map();

    let ChainCryptoClass = require(this.options.chainCryptoLibPath);

    this.chainCrypto = new ChainCryptoClass({
      this.options.chainSymbol,
      {
        chainModuleAlias: this.options.chainModuleAlias,
        passphrase: this.options.passphrase,
        multisigAddress: this.options.multisigAddress,
        memberAddress: this.options.memberAddress,
        keyIndexDirPath: this.options.keyIndexDirPath
      },
      logger: this.logger
    });
  }

  get dependencies() {
    return ['app', 'network', this.options.chainModuleAlias];
  }

  static get alias() {
    return DEFAULT_MODULE_ALIAS;
  }

  static get info() {
    return {
      author: 'Jonathan Gros-Dubois',
      version: packageJSON.version,
      name: DEFAULT_MODULE_ALIAS,
    };
  }

  static get migrations() {
    return [];
  }

  get events() {
    return [
      'bootstrap',
    ];
  }

  get actions() {
    return {
      getStatus: {
        handler: () => {
          return {
            version: packageJSON.version
          };
        }
      }
    };
  }

  _getSignatureQuota(targetChain, transaction) {
    return transaction.signatures.length - this.multisigWalletInfo.requiredSignatureCount;
  }

  async _verifySignature(targetChain, transaction, signaturePacket) {
    let hasMemberAddress = this.multisigWalletInfo.members.has(signaturePacket.signerAddress);
    if (!hasMemberAddress) {
      return false;
    }
    return this.chainCrypto[targetChain].verifyTransactionSignature(transaction, signaturePacket);
  }

  async _processSignature(signatureData) {
    let { signaturePacket, transactionId } = signatureData;
    if (!signaturePacket) {
      signaturePacket = {};
    }
    let transfer = this.pendingTransfers.get(transactionId);
    let { signerAddress } = signaturePacket;
    if (!transfer) {
      throw new Error(
        `Could not find a pending transfer ${
          transactionId
        } to match the signature from the signer ${
          signerAddress
        }`
      );
    }
    let { transaction, processedSignerAddressSet, targetChain } = transfer;
    if (processedSignerAddressSet.has(signerAddress)) {
      throw new Error(
        `A signature from the signer ${
          signerAddress
        } has already been received for the transaction ${
          transactionId
        }`
      );
    }

    let isValidSignature = await this._verifySignature(targetChain, transaction, signaturePacket);
    if (!isValidSignature) {
      throw new Error(
        `The signature from the signer ${
          signerAddress
        } for the transaction ${
          transactionId
        } was invalid or did not correspond to the account in its current state`
      );
    }

    processedSignerAddressSet.add(signerAddress);
    transaction.signatures.push(signaturePacket);

    let signatureQuota = this._getSignatureQuota(targetChain, transaction);
    if (signatureQuota >= 0 && transfer.readyTimestamp == null) {
      transfer.readyTimestamp = Date.now();
    }
  }

  expireMultisigTransactions() {
    let now = Date.now();
    for (let [txnId, transfer] of this.pendingTransfers) {
      if (now - transfer.timestamp < this.options.multisigExpiry) {
        break;
      }
      this.pendingTransfers.delete(txnId);
    }
  }

  flushPendingMultisigTransactions() {
    let transactionsToBroadcastPerChain = {};
    let now = Date.now();

    for (let transfer of this.pendingTransfers.values()) {
      if (transfer.readyTimestamp != null && transfer.readyTimestamp + this.multisigReadyDelay <= now) {
        if (!transactionsToBroadcastPerChain[transfer.targetChain]) {
          transactionsToBroadcastPerChain[transfer.targetChain] = [];
        }
        transactionsToBroadcastPerChain[transfer.targetChain].push(transfer.transaction);
      }
    }

    let chainSymbolList = Object.keys(transactionsToBroadcastPerChain);
    for (let chainSymbol of chainSymbolList) {
      let maxBatchSize = this.options.multisigMaxBatchSize;
      let transactionsToBroadcast = transactionsToBroadcastPerChain[chainSymbol].slice(0, maxBatchSize);
      this._broadcastTransactionsToChain(chainSymbol, transactionsToBroadcast);
    }
  }

  flushPendingSignatures() {
    let signaturesToBroadcast = [];

    for (let transfer of this.pendingTransfers.values()) {
      for (let signaturePacket of transfer.transaction.signatures) {
        signaturesToBroadcast.push({
          signaturePacket,
          transactionId: transfer.transaction.id
        });
      }
    }

    if (signaturesToBroadcast.length) {
      let maxBatchSize = this.options.signatureMaxBatchSize;
      this._broadcastSignaturesToSubnet(
        signaturesToBroadcast.slice(0, maxBatchSize)
      );
    }
  }

  async _broadcastTransactionsToChain(targetChain, transactions) {
    for (let transaction of transactions) {
      try {
        await this.channel.invoke(`${this.options.chainModuleAlias}:postTransaction`, {
          transaction
        });
      } catch (error) {
        this.logger.error(
          `Error encountered while attempting to post transaction ${
            transaction.id
          } to the ${targetChain} network - ${error.message}`
        );
      }
    }
  }

  async load(channel) {
    this.channel = channel;

    await this.chainCrypto.load(channel);

    this._multisigExpiryInterval = setInterval(() => {
      this.expireMultisigTransactions();
    }, this.options.multisigExpiryCheckInterval);

    this._multisigFlushInterval = setInterval(() => {
      this.flushPendingMultisigTransactions();
    }, this.options.multisigFlushInterval);

    this._signatureFlushInterval = setInterval(() => {
      this.flushPendingSignatures();
    }, this.options.signatureFlushInterval);

    await this.channel.invoke('app:updateModuleState', {
      [this.alias]: {} // TODO 222
    });

    let hasMultisigWalletsInfo = false;

    this.channel.subscribe(`network:event:${this.alias}:signatures`, async ({data}) => {
      if (!hasMultisigWalletsInfo) {
        return;
      }
      let signatureDataList = Array.isArray(data) ? data.slice(0, this.options.signatureMaxBatchSize) : [];
      await Promise.all(
        signatureDataList.map(async (signatureData) => {
          try {
            await this._processSignature(signatureData || {});
          } catch (error) {
            this.logger.debug(
              `Failed to process signature because of error: ${error.message}`
            );
          }
        })
      );
    });

    let loadMultisigWalletInfo = async () => {
      return Promise.all(
        this.chainSymbols.map(async (chainSymbol) => {
          let multisigMembers = await this._getMultisigWalletMembers(chainSymbol, this.options.multisigAddress);
          let multisigMemberSet = new Set(multisigMembers);
          this.multisigWalletInfo.members = multisigMemberSet;
          this.multisigWalletInfo.memberCount = multisigMemberSet.size;
          this.multisigWalletInfo.requiredSignatureCount = await this._getMinMultisigRequiredSignatures(chainSymbol, this.options.multisigAddress);
        })
      );
    };

    channel.publish(`${this.alias}:bootstrap`);
  }

  _sha1(string) {
    return crypto.createHash('sha1').update(string).digest('hex');
  }

  _transactionComparator(a, b) {
    // The sort order cannot be predicted before the block is forged.
    if (a.sortKey < b.sortKey) {
      return -1;
    }
    if (a.sortKey > b.sortKey) {
      return 1;
    }

    // This should never happen unless there is a hash collision.
    this.logger.error(
      `Failed to compare transactions ${
        a.id
      } and ${
        b.id
      } from block ID ${
        blockId
      } because they had the same sortKey - This may lead to nondeterministic output`
    );
    return 0;
  }

  async _getMultisigWalletMembers(chainSymbol, walletAddress) {
    return this.channel.invoke(`${this.options.moduleAlias}:getMultisigWalletMembers`, {walletAddress});
  }

  async _getMinMultisigRequiredSignatures(chainSymbol, walletAddress) {
    return this.channel.invoke(`${this.options.moduleAlias}:getMinMultisigRequiredSignatures`, {walletAddress});
  }

  async _getOutboundTransactions(chainSymbol, walletAddress, fromTimestamp, limit) {
    return this.channel.invoke(`${this.options.moduleAlias}:getOutboundTransactions`, {walletAddress, fromTimestamp, limit});
  }

  async _getInboundTransactionsFromBlock(chainSymbol, walletAddress, blockId) {
    let txns = await this.channel.invoke(`${this.options.moduleAlias}:getInboundTransactionsFromBlock`, {walletAddress, blockId});

    let transactions = txns.map(txn => ({
      ...txn,
      sortKey: this._sha1(txn.id + blockId)
    })).sort((a, b) => this._transactionComparator(a, b));

    return transactions;
  }

  async _getOutboundTransactionsFromBlock(chainSymbol, walletAddress, blockId) {
    let txns = await this.channel.invoke(`${this.options.moduleAlias}:getOutboundTransactionsFromBlock`, {walletAddress, blockId});

    let transactions = txns.map(txn => ({
      ...txn,
      sortKey: this._sha1(txn.id + blockId)
    })).sort((a, b) => this._transactionComparator(a, b));

    return transactions;
  }

  async _getLastBlockAtTimestamp(chainSymbol, timestamp) {
    return this.channel.invoke(`${this.options.moduleAlias}:getLastBlockAtTimestamp`, {timestamp});
  }

  async _getMaxBlockHeight(chainSymbol) {
    return this.channel.invoke(`${this.options.moduleAlias}:getMaxBlockHeight`, {});
  }

  async _getBlocksBetweenHeights(chainSymbol, fromHeight, toHeight, limit) {
    return this.channel.invoke(`${this.options.moduleAlias}:getBlocksBetweenHeights`, {fromHeight, toHeight, limit});
  }

  async _getBlockAtHeight(chainSymbol, height) {
    return this.channel.invoke(`${this.options.moduleAlias}:getBlockAtHeight`, {height});
  }

  async _getBaseChainBlockTimestamp(height) {
    let firstBaseChainBlock = await this._getBlockAtHeight(this.baseChainSymbol, height);
    return firstBaseChainBlock.timestamp;
  };

  // Broadcast the signature to all DEX nodes with a matching baseAddress and quoteAddress
  async _broadcastSignaturesToSubnet(signatureDataList) {
    let actionRouteString = `${this.alias}?baseAddress=${this.baseAddress}&quoteAddress=${this.quoteAddress}`;
    try {
      await this.channel.invoke('network:emit', {
        event: `${actionRouteString}:signatures`,
        data: signatureDataList
      });
    } catch (error) {
      this.logger.error(
        `Error encountered while attempting to broadcast signatures to the network - ${error.message}`
      );
    }
  }

  async execMultisigTransaction(targetChain, transactionData, message, extraTransferData) {
    let chainTimestamp = this._denormalizeTimestamp(targetChain, transactionData.timestamp);
    let chainCrypto = this.chainCrypto[targetChain];
    let {
      transaction: preparedTxn,
      signature: multisigSignaturePacket
    } = await chainCrypto.prepareTransaction({
      recipientAddress: transactionData.recipientAddress,
      amount: transactionData.amount,
      fee: transactionData.fee,
      timestamp: chainTimestamp,
      message
    });

    let processedSignerAddressSet = new Set([multisigSignaturePacket.signerAddress]);
    preparedTxn.signatures.push(multisigSignaturePacket);

    // If the pendingTransfers map already has a transaction with the specified id, delete the existing entry so
    // that when it is re-inserted, it will be added at the end of the queue.
    // To perform expiry using an iterator, it's essential that the insertion order is maintained.
    if (this.pendingTransfers.has(preparedTxn.id)) {
      this.pendingTransfers.delete(preparedTxn.id);
    }
    let transfer = {
      id: preparedTxn.id,
      transaction: preparedTxn,
      recipientAddress: transactionData.recipientAddress,
      targetChain,
      processedSignerAddressSet,
      height: transactionData.height,
      timestamp: Date.now(),
      ...extraTransferData
    };
    this.pendingTransfers.set(preparedTxn.id, transfer);
  }

  async unload() {
    clearInterval(this._multisigExpiryInterval);
    clearInterval(this._multisigFlushInterval);
    clearInterval(this._signatureFlushInterval);
    await this.chainCrypto.unload();
  }
};

function wait(duration) {
  return new Promise((resolve) => {
    setTimeout(resolve, duration);
  });
}
