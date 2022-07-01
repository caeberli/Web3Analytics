import numpy as np
import pandas as pd
import requests

from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
import json


# Import Data
print("Load Description Embeddings")
desc_emb = np.load(
    "/home/dustin/Documents/Ducia/NFT_Embedding/data/desc_emb.npz"
)  # ("../data/desc_emb.npz")
traindata_desc_embeddings = dict(
    zip(("desc_embeddings"), (desc_emb[k] for k in desc_emb))
)["d"]
print(traindata_desc_embeddings.shape)

print("Load Description Labels, Address Labels and Name Labels")
NFT_attributes = np.load(
    "/home/dustin/Documents/Ducia/NFT_Embedding/data/NFT_attributes.npz"
)
d = dict(
    zip(
        ("description_list", "contract_addresses", "name_list"),
        (NFT_attributes[k] for k in NFT_attributes),
    )
)
description_list = d["description_list"]
contract_addresses = d["contract_addresses"]
name_list = d["name_list"]
print(description_list.shape)

train_desc_address = list(
    zip(contract_addresses, traindata_desc_embeddings, description_list)
)

# -----------------------------------------------------------------------------------------------

recommendation_list = []

OWNER_DICT = {
    "owner0": {
        "address": "0x91B3185d4e5679482796AFe384C00BB8506E3096",
        "nfts": [
            "0x92095e27a8BB6918665dc9f1bC4650d52feC77F0",
            "0x2086f6f916a6bf22920cb9b28fc4119ce245dff4",
            "0x22ca15d0817bac27696437cbba8143e54973f6c3",
        ],
        "ids": ["027D", "5CC2", "01C4"],
    },
    "owner1": {
        "address": "0xA858DDc0445d8131daC4d1DE01f834ffcbA52Ef1",
        "nfts": [
            "0x028faf7eab0d8abb4a2d784206bfa98979041ffc",
            "0x04e4b74b1730806ecdd1d9b9837e77546d40c3f0",
            "0x0540cd12951c6dc3b06bae466914b46dcaa28cf8",
        ],
        "ids": ["0011", "148B", "0007"],
    },
    "owner2": {
        "address": "0x45435e2aEE578EF60E11F8778dFEA69DC98BB946",
        "nfts": [
            "0x018befb7d1f3e84948466ef15cc46baf9ba5295f",
            "0x018befb7d1f3e84948466ef15cc46baf9ba5295f",
            "0x05e7f2499ff153fea2f20bbde0b5584c911c0af1",
        ],
        "ids": ["01C4", "14AA", "0045"],
    },
    "owner3": {
        "address": "0x1fEDFda87C3ad2D6449e31297C89D698F006919a",
        "nfts": [
            "0x2f8231e79e5d6510ba714511ff5a0c25ddf731b7",
            "0x3b2b05ef86ecc7ba225eee5247b767edfd2639b2",
            "0x78f3d9d866651e91167ed4073b40ab5c0f11cb5e",
        ],
        "ids": ["0165", "0002", "0002"],
    },
}

owners_dict = {}

for l, owner in enumerate(OWNER_DICT.keys()):

    print("Get Description of All NFTs of Owner")
    Owner_nft_fullAttributes = []
    Owner_Descriptions = []

    for p, nft in enumerate(OWNER_DICT[owner]["nfts"]):
        nft_contract_address = nft
        token_id = 1  # OWNER_DICT[owner]['ids'][p]
        URL2 = f"https://eth-mainnet.alchemyapi.io/v2/demo/getNFTMetadata?contractAddress={nft_contract_address}&tokenId={token_id}"
        NFT = requests.get(URL2)
        print(NFT)
        try:
            NFTjson = NFT.json()
        except:
            print(f"check1 not succeeded for {owner}_NFT{p}")
            continue
        if (
            type(NFTjson) is dict
            and "metadata" in NFTjson.keys()
            and type(NFTjson["metadata"]) is dict
            and "description" in NFTjson["metadata"].keys()
        ):
            description = NFTjson["metadata"]["description"]
            Owner_Descriptions.append(description)
            Owner_nft_fullAttributes.append(
                (nft_contract_address, token_id, description)
            )
            print(f"added Owner{owner}_NFT{p}")
        else:
            print(f"check2 not succeeded for {owner}_NFT{p}")

    # -----------------------------------------------------------------------------------------------------------

    print("Compute Recommendation for each NFT hold")
    # Option 1: User then chooses a specific NFT and we provide X (e.g. X = 5) recommendations
    number_recommendation = 6

    owner_descriptions = []
    for owner_nft in Owner_nft_fullAttributes:
        owner_descriptions.append(owner_nft[2])

    # Later on here we will just load out finetuned model from above
    model = SentenceTransformer("bert-base-nli-mean-tokens")

    # description embeddings
    nft_description_embeddings = model.encode(
        owner_descriptions,
        normalize_embeddings=True,
        show_progress_bar=True,
        batch_size=64,
    )

    owner_dict = {}

    for k, owner_nft in enumerate(
        Owner_nft_fullAttributes
    ):  # iterate through all nfts hold by owner
        nft_description = owner_nft[2]

        recommendations = []
        print(f"Compute Recommendation for NFT{k}")
        for i, j in enumerate(
            train_desc_address
        ):  # iterate through all other NFTs 11.000
            train_address = j[0]
            train_description_embedding = j[1]
            train_description = j[2]

            distance = np.sum(
                np.square((train_description_embedding - nft_description_embeddings[k]))
            )

            if i < number_recommendation and distance > 1e-6:
                recommendations.append(
                    (
                        distance,
                        train_description_embedding,
                        train_description,
                        train_address,
                    )
                )
                recommendations.sort(key=lambda x: x[0])
            elif distance > 1e-6:
                highest_distance = recommendations[X - 1][0]
                if distance < highest_distance and distance > 1e-6:
                    recommendations[number_recommendation - 1] = (
                        distance,
                        train_description_embedding,
                        train_description,
                        train_address,
                    )
                    recommendations.sort(key=lambda x: x[0])
        rec = []
        for m, recommendation in enumerate(recommendations):
            rec.append(recommendation[3])
        owner_dict[OWNER_DICT[owner]["nfts"][k]] = rec
        recommendation_list.append(recommendations)

    owners_dict[OWNER_DICT[owner]["address"]] = owner_dict

print("Write Data")
with open("../data/data_recommendation.json", "w") as f:
    json.dump(owners_dict, f)
