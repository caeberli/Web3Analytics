import React from "react";
import Head from "next/head";
import Image from "next/image";
import { useState, useEffect } from "react";
import { DataGrid } from "@material-ui/data-grid";
import myData from "../../Data/target_users.json";
import { NFTCard } from "./nftCard.jsx";
import mypic1 from "../public/images/NFT_1.gif";
import mypic2 from "../public/images/NFT_2.jpg";
import mypic3 from "../public/images/NFT_3.gif";
import mypic4 from "../public/images/NFT_4.jpg";
import { withStyles, makeStyles } from "@material-ui/core/styles";
import styles from "./Test.module.css";
import { mintNFT } from "./util/minter.js";
import { pinJSONToIPFS } from "./util/pinata.js";

const useStyles = makeStyles({
  root: {
    backgroundColor: "#F4EFE9",
  },
  gradtext: {
    backgroundImage: "linearGradient(to bottom, #FF005B, #FFD4A9)",
    WebkitBackgroundClip: "text",
    WebkitTextFillColor: "transparent",
  },
});

export default function Home() {
  const classes = useStyles();
  const [description, setDescription] = useState(
    "Hey Basketball Fan! Our mission is to own an NBA team and we would love to have you join us and become part of the team 🏀 🚀"
  );

  const [listSent, setListSent] = useState([]);
  const [selectionModel, setSelectionModel] = useState([]);
  const [background1, setBackground1] = useState("white");
  const [checked, setChecked] = useState("");

  async function changeDescription() {
    setDescription(document.getElementById("desc").value);
  }

  async function mint(add, tokenURI) {
    const raw = await mintNFT(add, tokenURI);
    return raw;
  }

  async function executeDummy() {
    const metadata = new Object();
    metadata.name = name;
    metadata.image = checked;
    metadata.description = description;

    const pinataResponse = await pinJSONToIPFS(metadata);
    if (!pinataResponse.success) {
      return {
        success: false,
        status: "😢 Something went wrong while uploading your tokenURI.",
      };
    }
    const tokenURI = pinataResponse.pinataUrl;

    const add = "0x71c619aB40e05378CEd1Ca46942d9B23C2bdE709";
    const raw = mint(add, tokenURI);
    console.log("Raw: ", raw);
  }

  async function execute() {
    const metadata = new Object();
    metadata.name = name;
    metadata.image = checked;
    metadata.description = description;

    const pinataResponse = await pinJSONToIPFS(metadata);
    if (!pinataResponse.success) {
      return {
        success: false,
        status: "😢 Something went wrong while uploading your tokenURI.",
      };
    }
    const tokenURI = pinataResponse.pinataUrl;

    selectionModel.map((nft_id, index) => {
      const add = rows[nft_id]["address"];
      const raw = mint(add, tokenURI);
      console.log("Raw: ", raw);
    });
  }

  const columns = [
    {
      field: "id",
      headerName: "Account Number",
      width: "200",
      align: "left",
    },
    {
      field: "address",
      headerName: "Wallet Address",
      sortable: false,
      width: "400",
      align: "left",
    },
    {
      field: "sent",
      headerName: "Already Sent",
      width: "200",
      type: "bool",
    },
    {
      field: "balance",
      headerName: "Balance (ETH)",
      width: "170",
      type: "number",
    },
    {
      field: "nfts",
      headerName: "Number of Basketball NFTs",
      width: "300",
      type: "number",
    },
  ];

  const rows = Object.keys(myData).map((add, index) => {
    return {
      id: index,
      address: add,
      sent: listSent.includes(add) ? true : false,
      balance: myData[add][1].toFixed(2),
      nfts: myData[add][0],
    };
  });

  return (
    <div
      style={{
        backgroundColor: "#0F0F12",
        minHeight: "100vh",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div>
        <Head>
          <link rel="stylesheet" href="https://use.typekit.net/fox1qde.css" />
        </Head>
      </div>

      <div
        style={{
          display: "flex",
          justifyContent: "center",
          flexDirection: "column",
          marginLeft: "120px",
          marginRight: "120px",
        }}
      >
        <h1
          style={{
            fontSize: "80px",
            fontFamily: "SK Cuber",
            color: "white",
            textAlign: "center",
          }}
        >
          🏀 Krause House Airdrop Tool 🏀
        </h1>
      </div>

      <div
        style={{
          display: "flex",
          justifyContent: "flex-start",
          alignItems: "center",
          flexDirection: "row",
          marginTop: "40px",
          fontFamily: "headline-gothic-atf",
          marginLeft: "120px",
          marginRight: "120px",
        }}
      >
        <h1
          className={styles.test}
          style={{
            fontSize: "50px",
            display: "inline",
            float: "left",
          }}
        >
          Step 1:
        </h1>
        <h1
          style={{
            fontSize: "50px",
            color: "#F4EFE9",
            display: "inline",
          }}
        >
          &nbsp; Select The Addresses to Target
        </h1>
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "center",
          flexDirection: "column",
          alignItems: "center",
          marginTop: "15px",
          marginLeft: "120px",
          marginRight: "120px",
        }}
      >
        <div
          style={{
            height: "370px",
            fontSize: "10px",
            color: "white",
            width: "100%",
          }}
        >
          <DataGrid
            className={classes.root}
            rows={rows}
            columns={columns}
            pageSize={5}
            rowsPerPageOptions={[5]}
            isRowSelectable={(params) => params.row.sent == false}
            checkboxSelection
            onSelectionModelChange={(newSelectionModel) => {
              setSelectionModel(newSelectionModel);
            }}
            selectionModel={selectionModel}
          />
        </div>
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "flex-end",
          alignItems: "center",
          flexDirection: "row",
          marginTop: "40px",
          fontFamily: "headline-gothic-atf",
          marginLeft: "120px",
          marginRight: "120px",
        }}
      >
        <h1
          className={styles.test}
          style={{
            fontSize: "50px",
            display: "inline",
          }}
        >
          Step 2:
        </h1>
        <h1
          style={{
            fontSize: "50px",
            color: "#F4EFE9",
            display: "inline",
          }}
        >
          &nbsp; Choose the NFT Design
        </h1>
      </div>

      <div
        style={{
          display: "flex",
          flexDirection: "row",
          justifyContent: "center",
          gridGap: "20px",
          marginTop: "15px",
          marginLeft: "120px",
          marginRight: "120px",
        }}
      >
        <NFTCard
          index={0}
          pic={mypic1}
          checked={checked}
          setChecked={setChecked}
        ></NFTCard>
        <NFTCard
          index={1}
          pic={mypic2}
          checked={checked}
          setChecked={setChecked}
        ></NFTCard>
        <NFTCard
          index={2}
          pic={mypic3}
          checked={checked}
          setChecked={setChecked}
        ></NFTCard>
        <NFTCard
          index={3}
          pic={mypic4}
          checked={checked}
          setChecked={setChecked}
        ></NFTCard>
      </div>

      <div
        style={{
          display: "flex",
          justifyContent: "flex-start",
          alignItems: "center",
          flexDirection: "row",
          marginTop: "40px",
          fontFamily: "headline-gothic-atf",
          marginLeft: "120px",
          marginRight: "120px",
        }}
      >
        <h1
          className={styles.test}
          style={{
            fontSize: "50px",
            display: "inline",
          }}
        >
          Step 3:
        </h1>
        <h1
          style={{
            fontSize: "50px",
            color: "#F4EFE9",
            display: "inline",
          }}
        >
          &nbsp; Choose the Description Message
        </h1>
      </div>

      <div
        style={{
          display: "flex",
          justifyContent: "center",
          flexDirection: "column",
          alignItems: "flex-start",
          marginTop: "5px",
          marginLeft: "120px",
          marginRight: "120px",
        }}
      >
        <p style={{ color: "#F4EFE9" }}>
          {" "}
          Feel free to modify the Placeholder:
        </p>
        <textarea
          id="desc"
          style={{ width: "100%", borderRadius: "4px", height: "90px" }}
          defaultValue="Hey Basketball Fan! Our mission is to own an NBA team and we would
          love to have you join us and become part of the team 🏀 🚀"
          onChange={() => changeDescription()}
        ></textarea>
      </div>

      <div
        style={{
          display: "flex",
          justifyContent: "flex-end",
          alignItems: "center",
          flexDirection: "row",
          marginTop: "40px",
          fontFamily: "headline-gothic-atf",
          marginLeft: "120px",
          marginRight: "120px",
        }}
      >
        <h1
          className={styles.test}
          style={{
            fontSize: "50px",
            display: "inline",
          }}
        >
          Step 4:
        </h1>
        <h1
          style={{
            fontSize: "50px",
            color: "#F4EFE9",
            display: "inline",
          }}
        >
          &nbsp; Mint and Grow the DAO!
        </h1>
      </div>

      <div
        style={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          flexDirection: "column",
          marginBottom: "40px",
          marginBottom: "40px",
        }}
      >
        <div>
          <button
            className={styles.btn}
            style={{
              height: "120px",
              width: "350px",
              fontSize: "30px",
              marginRight: "30px",
              fontFamily: "SK Cuber",
            }}
            onClick={() => executeDummy()}
          >
            Execute Airdrop!
          </button>
          <button
            className={styles.btn2}
            style={{
              height: "120px",
              width: "350px",
              fontSize: "30px",
              marginRight: "30px",
              fontFamily: "SK Cuber",
            }}
            onClick={() => executeDummy()}
          >
            Track Conversion
          </button>
        </div>
      </div>
    </div>
  );
}
