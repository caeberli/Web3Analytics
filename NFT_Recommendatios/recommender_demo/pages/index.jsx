import Head from "next/head";
import Image from "next/image";
import { useState, useEffect, useRef } from "react";
import { NFTCard } from "./Components/nftCard.jsx";

const Home = () => {
  const [wallet, setWalletAddress] = useState("");
  const [userSelected, setUserSelected] = useState(false);
  const [NFTs, setNFTs] = useState([]);
  const [userNFTs, setUserNFTs] = useState(true);
  const [contractAddressList, setContractAddressList] = useState([]);
  const [recAdd, setrecAdd] = useState("");
  const [NFTRecommendations, setNFTRecommendations] = useState([]);
  const [inputDescription, setInputDescription] = useState("");

  const fetchNFTs = async () => {
    let nfts;
    var requestOptions = {
      method: "GET",
    };
    const api_key = "QptlBSlLspchzOK2mEHRDogveEdOGqNO";
    const baseURL = `https://eth-mainnet.alchemyapi.io/v2/${api_key}/getNFTs/`;
    const fetchURL = `${baseURL}?owner=${wallet}`;

    nfts = await fetch(fetchURL, requestOptions).then((data) => data.json());

    if (nfts) {
      console.log("nfts:", nfts);
      setNFTs(nfts.ownedNfts);
    }
  };

  const callApiAsync = async (current_address) => {
    var requestOptions = {
      method: "GET",
    };
    const api_key = "QptlBSlLspchzOK2mEHRDogveEdOGqNO";
    const baseURL = `https://eth-mainnet.alchemyapi.io/nft/v2/${api_key}/getNFTMetadata`;

    const tokenId = "1";
    const fetchURL = `${baseURL}?contractAddress=${current_address}&tokenId=${tokenId}`;
    let rec_nft;
    rec_nft = await fetch(fetchURL, requestOptions).then((data) => data.json());
    console.log("hdhd", NFTRecommendations);
    setNFTRecommendations((NFTRecommendations) => [
      ...NFTRecommendations,
      rec_nft,
    ]);
  };

  const CreateRecommendations = async () => {
    setNFTRecommendations([]);
    contractAddressList.length &&
      contractAddressList.map((current_address) => {
        callApiAsync(current_address);
      });
  };

  useEffect(() => {
    setUserSelected(false);
    CreateRecommendations();
  }, [contractAddressList]);

  console.log("usernfts", userNFTs);

  return (
    <div
      style={{
        backgroundImage: "linear-gradient(to bottom, #FFFFFF, #a0edff)",
        minHeight: "100vh",
      }}
    >
      <div className="flex flex-col items-center justify-center py-8 gap-y-3">
        <h1 style={{ fontSize: "50px" }}> Find Your Perfect NFT! </h1>
        <div className="flex flex-col w-full justify-center items-center gap-y-2">
          <select
            name="selected_address"
            type="text"
            onChange={(e) => {
              setWalletAddress(e.target.value);
              console.log(wallet);
            }}
            value={wallet}
            className="w-2/5 bg-slate-100 py-2 px-2 rounded-lg text-gray-800 focus:outline-blue-300 disabled:bg-slate-50 disabled:text-gray-50"
          >
            <option value="" disabled selected>
              Select a Wallet Address
            </option>
            <option>0x376A273cFe9512b9f60304D289eccFE6905c020a</option>
            <option>0xA858DDc0445d8131daC4d1DE01f834ffcbA52Ef1</option>
            <option>0x45435e2aEE578EF60E11F8778dFEA69DC98BB946</option>
            <option>0x1fEDFda87C3ad2D6449e31297C89D698F006919a</option>
          </select>
          <button
            className={
              "disabled:bg-slate-500 text-white bg-blue-400 px-4 py-2 mt-3 rounded-sm w-1/5"
            }
            style={{ backgroundColor: "#a0edff", color: "#000000" }}
            onClick={() => {
              fetchNFTs();
              setUserNFTs(true);
              setUserSelected(true);
            }}
          >
            Show my NFTs
          </button>

          {!userSelected && !userNFTs ? (
            <div>
              {" "}
              <h1 className={"mt-3"} style={{ fontSize: "20px" }}>
                {" "}
                Enjoy your Deep Learning Recommendations{" "}
              </h1>{" "}
            </div>
          ) : null}
        </div>

        <div className="flex flex-wrap gap-y-12 mt-4 w-5/6 gap-x-2 justify-center">
          {NFTs.filter((item, idx) => item.media[0].gateway.length > 0)
            .filter((item, idx) => idx < 3)
            .map((nft) => {
              //console.log(nft.media[0].gateway)
              //console.log(typeof(nft.media[0].gateway))
              if (userNFTs == true) {
                return (
                  <NFTCard
                    nft={nft}
                    recAdd={recAdd}
                    setrecAdd={setrecAdd}
                    NFTRecommendations={NFTRecommendations}
                    setNFTRecommendations={setNFTRecommendations}
                    contractAddressList={contractAddressList}
                    setContractAddressList={setContractAddressList}
                    inputDescription={inputDescription}
                    userNFTs={userNFTs}
                    setInputDescription={setInputDescription}
                    setUserNFTs={setUserNFTs}
                    wallet={wallet}
                  ></NFTCard>
                );
              }
            })}
        </div>

        <div className="flex flex-wrap gap-y-12 mt-4 w-5/6 gap-x-2 justify-center">
          {NFTRecommendations.filter(
            (item, idx) => item.media[0].gateway.length > 0
          )
            .filter((item, idx) => idx < 10)
            .map((nft) => {
              //console.log(nft.media[0].gateway)
              //console.log(typeof(nft.media[0].gateway))
              if (nft.media[0].gateway.length > 0 && userNFTs == false) {
                return (
                  <NFTCard
                    nft={nft}
                    recAdd={recAdd}
                    setrecAdd={setrecAdd}
                    NFTRecommendations={NFTRecommendations}
                    setNFTRecommendations={setNFTRecommendations}
                    contractAddressList={contractAddressList}
                    setContractAddressList={setContractAddressList}
                    inputDescription={inputDescription}
                    setInputDescription={setInputDescription}
                    userNFTs={userNFTs}
                    setUserNFTs={setUserNFTs}
                    wallet={wallet}
                  ></NFTCard>
                );
              }
            })}
        </div>
      </div>
    </div>
  );
};

export default Home;
