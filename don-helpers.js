
const { SecretsManager } = require("@chainlink/functions-toolkit");
const { ethers } = require("ethers");

class FunctionsDonUtils {
  constructor(config) {
    this.signerConfig = config.signer; // Store raw config for provider construction
    this.routerAddress = config.functionsRouterAddress;
    this.donId = config.donId;
    this.secretsManager = null;
  }

  async initialize() {
    const provider = new ethers.providers.JsonRpcProvider(this.signerConfig.providerUrl);
    const wallet = new ethers.Wallet(this.signerConfig.privateKey, provider);

    this.secretsManager = new SecretsManager({
      signer: wallet,
      functionsRouterAddress: this.routerAddress,
      donId: this.donId,
      gatewayUrls: [
        "https://01.functions-gateway.testnet.chain.link/",
        "https://02.functions-gateway.testnet.chain.link/",
        "https://03.functions-gateway.testnet.chain.link/"
      ],
    });

    await this.secretsManager.initialize();
  }

  async encryptSecrets(secretsObject) {
  const { encryptedSecrets } = await this.secretsManager.encryptSecrets(secretsObject);
  return encryptedSecrets;
}


  async uploadEncryptedSecrets({ encryptedSecretsHexstring, slotId = 0 }) {
    const result = await this.secretsManager.uploadEncryptedSecretsToDON({
      encryptedSecretsHexstring,
      slotId,
      minutesUntilExpiration: 30,
      gatewayUrls: [
        "https://01.functions-gateway.testnet.chain.link/",
        "https://02.functions-gateway.testnet.chain.link/",
        "https://03.functions-gateway.testnet.chain.link/"
      ]
    });

    return {
      encryptedSecretsReference: result,
    };
  }
}

module.exports = {
  FunctionsDonUtils,
};
