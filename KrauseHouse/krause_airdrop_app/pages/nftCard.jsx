import React from "react";
import { useState, useEffect, useRef } from "react";
import Image from "next/image";
import SportsBasketballIcon from "@mui/icons-material/SportsBasketball";
import Checkbox from "@mui/material/Checkbox";

export const NFTCard = ({ index, pic, checked, setChecked }) => {
  const image_urls = [
    "https://ipfs.io/ipfs/QmVkeHRawmKAK8yLBjHwGDHQ2XWKWNPqSYM8Fqcpk9m2YP?filename=NFT_1.gif",
    "https://ipfs.io/ipfs/QmPhkFLfRxwmqca7twP9ZsV3VcNi3u74eg36d7KvDbpdpg?filename=NFT_2.jpg",
    "https://ipfs.io/ipfs/QmWDiToGNKUJ2eDStMTHaV5DzuKWKcmfDAmkEBHj5QUmgE?filename=NFT_3.gif",
    "https://ipfs.io/ipfs/Qmck4RTiYpdp7m6M47Nc6QRQvdpqRhydmpG52BncaKAdDL?filename=NFT_4.jpg",
  ];

  const handleChange = (event) => {
    if (event.target.checked) {
      setChecked(image_urls[index]);
    }
  };
  return (
    <div style={{ width: "25%", display: "flex", flexDirection: "column" }}>
      <div style={{ borderRadius: "6px" }}>
        <Image
          height={2000}
          width={1600}
          style={{
            borderTopLeftRadius: "6px",
            borderTopRightRadius: "6px",
          }}
          src={pic}
        />
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            borderBottomLeftRadius: "6px",
            borderBottomRightRadius: "6px",
            height: "110px",
            paddingLeft: "10px",
            paddingRight: "10px",
            paddingBottom: "10px",
            paddingTop: "10px",
            backgroundColor: "#fe025a",
          }}
        >
          <Checkbox
            checked={checked == image_urls[index]}
            onChange={handleChange}
            sx={{
              "& .MuiSvgIcon-root": {
                fontSize: "60px",
              },
            }}
            icon={<SportsBasketballIcon />}
            checkedIcon={
              <SportsBasketballIcon
                sx={{
                  color: "white",
                }}
              />
            }
          />

          {checked == image_urls[index] ? (
            <h3
              style={{
                fontSize: "17px",
                fontFamily: "SK Cuber",
                color: "white",
                display: "flex",
                justifyContent: "center",
              }}
            >
              {" "}
              Selected!{" "}
            </h3>
          ) : (
            <h3
              style={{
                fontSize: "17px",
                fontFamily: "SK Cuber",
                color: "#75092a",
                display: "flex",
                justifyContent: "center",
              }}
            >
              {" "}
              Select NFT{" "}
            </h3>
          )}
        </div>
      </div>
    </div>
  );
};
