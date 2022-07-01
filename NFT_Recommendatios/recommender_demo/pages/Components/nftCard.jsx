import React from "react";
import { useState, useEffect, useRef } from "react";
import myData from "../../data_recommendation.json";

export const NFTCard = ({
  nft,
  recAdd,
  setrecAdd,
  NFTRecommendations,
  setNFTRecommendations,
  contractAddressList,
  setContractAddressList,
  inputDescription,
  setInputDescription,
  userNFTs,
  setUserNFTs,
  wallet,
}) => {
  return (
    <div className="w-1/4 flex flex-col ">
      <div className="rounded-md">
        <img
          className="object-cover h-128 w-full rounded-t-md"
          src={nft.media[0].gateway}
        ></img>
      </div>
      <div className="flex flex-col y-gap-2 px-2 py-3 bg-slate-100 rounded-b-md h-110 ">
        <div className="">
          <h2 className="text-xl text-gray-800">{nft.title}</h2>
          <p className="text-gray-600">
            NFT Id:{" "}
            {nft.id.tokenId
              .substr(nft.id.tokenId.indexOf("x") + 1)
              .slice(-6)
              .replace(/^0+/, "")}
          </p>
          <p className="text-gray-600">
            Collection Address:
            {` ${nft.contract.address.substr(
              0,
              4
            )}...${nft.contract.address.substr(
              nft.contract.address.length - 4
            )}`}
          </p>
        </div>
        <div className="flex-grow">
          <p className="text-gray-600">
            Description:{" "}
            {nft.description === undefined
              ? "NA"
              : nft.description === ""
              ? "NA"
              : ` ${nft.description.substr(0, 150)}... `}
          </p>
        </div>
        {userNFTs ? (
          <div className="flex justify-center mb-1 mt-4">
            <button
              style={{ backgroundColor: "#a0edff", color: "#000000" }}
              className="py-2 px-4 bg-blue-500 w-1/2 text-center rounded-m text-white cursor-pointer"
              onClick={() => {
                let recs;
                recs = myData[wallet][nft.contract.address];
                setContractAddressList(recs);
                setUserNFTs(false);
              }}
            >
              See Similar NFTs
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
};
