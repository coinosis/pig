const dbModule = require('./db.js');
const Web3 = require('web3');
const ProxyEvent = require('./ProxyEvent.abi.json');
const fetch = require('node-fetch');

let db, web3xDAI, web3ETH;
const initialize = async () => {
  await dbModule.connect();
  db = dbModule.getCollections();
  web3xDAI = new Web3('https://xdai.poanetwork.dev');
  const infuraURL = 'wss://mainnet.infura.io/ws/v3';
  const infuraKey = 'f81483dac9a84ecda3861bf2d19d693c';
  web3ETH = new Web3(`${infuraURL}/${infuraKey}`);
}
const halfDay = 60 * 60 * 12;
const apiEndpoint = 'https://min-api.cryptocompare.com/data/v2/histoday';
const apiOptions = '?fsym=ETH&tsym=USD&limit=1&aggregate=1';
const key = 'c7f3fa8b12bc087368f4d8d1a4a2f1b1f8f54fbb9aeb3d6edeee3c61bac94fe2';
const apiURL = `${apiEndpoint}${apiOptions}&api_key=${key}`;

// taken from https://stackoverflow.com/a/51643788/2430274
const formatDate = timestamp => {
  const now = new Date();
  const offset = now.getTimezoneOffset() * 60;
  const localTimestamp = timestamp - offset;
  const date = new Date(localTimestamp * 1000);
  const iso = date.toISOString();
  const isoish = iso.replace('T', ' ').slice(0, 19);
  return isoish;
}

const main = async () => {
  await initialize();
  const entries = [];
  const events = await db.events.find({ version: 1 }).toArray();
  for (const event of events) {
    let address;
    const { currency, version } = event;
    if (version === 1) {
      address = '0xBA77007e5745f7e6F80F78904c7628e30909F828';
    } else {
      address = event.address;
    }
    let instance;
    if (currency === 'ETH') {
      instance = new web3ETH.eth.Contract(ProxyEvent, address);
    } else {
      instance = new web3xDAI.eth.Contract(ProxyEvent, address);
    }
    // para eventos versión 1
    // const subscription = instance.events.Transfer(
    //   { fromBlock: 0 },
    // );
    // no sé cómo llegar de la subscripción a los eventos
    const transfers = await instance.getPastEvents('Transfer', {
      fromBlock: 0,
    });
    for (const transfer of transfers) {
      const { attendee: address, reward: rewardWei } = transfer.returnValues;
      const reward = web3ETH.utils.fromWei(rewardWei);
      let block;
      if (currency === 'ETH') {
        block = await web3ETH.eth.getBlock(transfer.blockHash);
      } else {
        block = await web3xDAI.eth.getBlock(transfer.blockHash);
      }
      const { timestamp } = block;
      const userInfo = await db.users.findOne({ address });
      const user = userInfo && userInfo.name;
      const entry = {
        event: event.url,
        date: formatDate(timestamp),
        user,
        address,
        reward,
        currency,
      };
      if (currency === 'ETH') {
        const maxTimestamp = BigInt(timestamp) + BigInt(halfDay);
        const apiRequest = `${apiURL}&toTs=${maxTimestamp}`;
        const response = await fetch(apiRequest);
        const info = await response.json();
        const data = info.Data.Data;
        entry.currencyPriceUSD = data[data.length - 1].open;
        entry.rewardUSD = reward * entry.currencyPriceUSD;
      } else {
        entry.currencyPriceUSD = 1;
        entry.rewardUSD = reward;
      }
      console.log(entry);
      entries.push(entry);
    }
  }
  await dbModule.disconnect();
}

main();
