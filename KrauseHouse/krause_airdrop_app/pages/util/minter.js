import { pinJSONToIPFS } from "./pinata.js";
import { ethers } from "ethers";
const contractABI = require("./contract-abi.json");

export const mintNFT = async (receiver, tokenURI) => {
  const alchemyProvider = new ethers.providers.AlchemyProvider(
    "matic",
    process.env.ALCHEMY_KEY
  );

  const feeData = await alchemyProvider.getFeeData();

  const signer = new ethers.Wallet(process.env.PRIVATE_KEY, alchemyProvider);

  const KrauseMintContract = new ethers.Contract(
    process.env.CONTRACT_ADDRESS,
    contractABI,
    signer
  );

  const tx = await KrauseMintContract.mintNft(receiver, tokenURI, {
    gasPrice: feeData.gasPrice,
  });

  console.log(tx);

  return { raw: tx };
};
