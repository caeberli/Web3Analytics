const axios = require("axios");

export const pinJSONToIPFS = async (JSONBody) => {
  const url = `https://api.pinata.cloud/pinning/pinJSONToIPFS`;
  //making axios POST request to Pinata ⬇️
  return axios
    .post(url, JSONBody, {
      headers: {
        Authorization:
          "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySW5mb3JtYXRpb24iOnsiaWQiOiJkMTdlMThlYy1lNTQ3LTQ1MWQtYjhiOS04NmYyM2M2ODA1MjMiLCJlbWFpbCI6ImNhZWJlcmxpQG1pdC5lZHUiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicGluX3BvbGljeSI6eyJyZWdpb25zIjpbeyJpZCI6IkZSQTEiLCJkZXNpcmVkUmVwbGljYXRpb25Db3VudCI6MX0seyJpZCI6Ik5ZQzEiLCJkZXNpcmVkUmVwbGljYXRpb25Db3VudCI6MX1dLCJ2ZXJzaW9uIjoxfSwibWZhX2VuYWJsZWQiOmZhbHNlLCJzdGF0dXMiOiJBQ1RJVkUifSwiYXV0aGVudGljYXRpb25UeXBlIjoic2NvcGVkS2V5Iiwic2NvcGVkS2V5S2V5IjoiZjE1NWQ5M2Y1NzBjOGRmOGIxZDYiLCJzY29wZWRLZXlTZWNyZXQiOiI1MDgzYzVjODg2OWQ4MDdhYzhiMGQ1YzM5NDIwZmUxNWZiNzJhNTM0YmY1NDJiMjE0OTYzMzNiOTk0M2E3YWYxIiwiaWF0IjoxNjU2MDc4MDczfQ.eajIZ2IaaoZD_dbQWI7NhLSZnJVyEPRLwfZ-36Z-qNY",
      },
    })
    .then(function (response) {
      return {
        success: true,
        pinataUrl:
          "https://gateway.pinata.cloud/ipfs/" + response.data.IpfsHash,
      };
    })
    .catch(function (error) {
      console.log(error);
      return {
        success: false,
        message: error.message,
      };
    });
};
