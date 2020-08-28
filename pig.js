const dbModule = require('./db.js');
const Web3 = require('web3');
const CoinosisV0 = require('./CoinosisV0.abi.json');
const CoinosisV1 = require('./Coinosis.abi.json');
const ProxyEvent = require('./ProxyEvent.abi.json');
const fetch = require('node-fetch');
const fs = require('fs');
const parse = require('csv-parse/lib/sync');
const stringify = require('csv-stringify/lib/sync');

let db, web3xDAI, web3ETH;
const initialize = async () => {
  await dbModule.connect();
  db = dbModule.getCollections();
  web3xDAI = new Web3('https://xdai.poanetwork.dev');
  const infuraURL = 'wss://mainnet.infura.io/ws/v3';
  const infuraKey = 'f81483dac9a84ecda3861bf2d19d693c';
  web3ETH = new Web3(`${infuraURL}/${infuraKey}`);
}
const ethPricesPath = './ethPrices.json';
const noContractPath = './noContract.csv';
const BCenLasA1 = 'Introducción a la blockchain para artistas';
const BCenLasA2 = 'La música en la cadena de bloques';
const time2event = {
  '1587666616': null,
  '1587684996': BCenLasA1,
  '1588288813': BCenLasA2,
};
const addressV0 = '0x3E824EEeD4Ad41a7dFb91F11bf8D39D0C6077B4f';
const addressV1 = '0xBA77007e5745f7e6F80F78904c7628e30909F828';
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

const getUserName = async address => {
  const userInfo = await db.users.findOne({ address });
  if (!userInfo) return '';
  const userName = userInfo.name;
  return userName;
}

const ethPrices = {};
let savedETHPrices;
try {
  savedETHPrices = JSON.parse(fs.readFileSync(ethPricesPath));
} catch (err) {
  savedETHPrices = {};
}
const getETHPriceUSD = async timestamp => {
  if (timestamp in savedETHPrices) return savedETHPrices[timestamp];
  if (timestamp in ethPrices) return ethPrices[timestamp];
  console.log(`fetching ETH price for date ${formatDate(timestamp)}...`);
  const maxTimestamp = BigInt(timestamp) + BigInt(halfDay);
  const apiRequest = `${apiURL}&toTs=${maxTimestamp}`;
  const response = await fetch(apiRequest);
  const info = await response.json();
  const data = info.Data.Data;
  const ethPriceUSD = data[data.length - 1].open;
  ethPrices[timestamp] = ethPriceUSD;
  return ethPriceUSD;
}

const getNoContractEntries = async () => {
  const stream = fs.readFileSync(noContractPath, { encoding: 'utf-8', });
  const data = parse(stream, { delimiter: ',', columns: true, });
  return data;
}

const getEntriesPreV2 = async (version) => {
  const entries = [];
  let hash2url;
  if (version === 1) {
    const events = await db.events.find({ version: 1 }).toArray();
    hash2url = events.reduce((hash2url, event) => {
      const { url } = event;
      const hash = web3ETH.utils.sha3(url);
      hash2url[hash] = url;
      return hash2url;
    }, {});
  }
  let instance;
  if (version === 1) {
    instance = new web3ETH.eth.Contract(CoinosisV1, addressV1);
  } else {
    instance = new web3ETH.eth.Contract(CoinosisV0, addressV0);
  }
  console.log(`fetching assessments for contract version ${version}...`);
  const assessments = await instance.getPastEvents('Assessment', {
    fromBlock: 0,
  });
  const blockInfo = assessments.reduce((blockInfo, assessment) => {
    const { blockHash, returnValues } = assessment;
    const { timestamp, eventURL } = returnValues;
    const url = version === 1
          ? hash2url[eventURL]
          : time2event[timestamp];
    blockInfo[blockHash] = { url, timestamp };
    return blockInfo;
  }, {});
  console.log(`fetching transfers for contract version ${version}...`);
  const transfers = await instance.getPastEvents('Transfer', { fromBlock: 0, });
  for (const transfer of transfers) {
    const { blockHash, returnValues } = transfer;
    const { url: event, timestamp } = blockInfo[blockHash];
    if (event === null) continue;
    const { addr: address, reward: rewardWei } = returnValues;
    const user = await getUserName(address);
    const reward = web3ETH.utils.fromWei(rewardWei);
    const currencyPriceUSD = await getETHPriceUSD(timestamp);
    const entry = {
      contractVersion: String(version),
      event,
      date: formatDate(timestamp),
      user,
      address,
      reward,
      currency: 'ETH',
      currencyPriceUSD: String(currencyPriceUSD),
      rewardUSD: String(reward * currencyPriceUSD),
    };
    entries.push(entry);
  }
  return entries;
}

const getEntriesV2 = async () => {
  const entries = [];
  const events = await db.events.find({ version: 2 }).toArray();
  for (const event of events) {
    const { address, currency } = event;
    let instance;
    if (currency === 'ETH') {
      instance = new web3ETH.eth.Contract(ProxyEvent, address);
    } else {
      instance = new web3xDAI.eth.Contract(ProxyEvent, address);
    }
    console.log(`fetching transfers for event ${event.url}...`);
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
      const user = await getUserName(address);
      const entry = {
        contractVersion: '2',
        event: event.url,
        date: formatDate(timestamp),
        user,
        address,
        reward,
        currency,
      };
      if (currency === 'ETH') {
        const currencyPriceUSD = await getETHPriceUSD(timestamp);
        entry.currencyPriceUSD = String(currencyPriceUSD);
        entry.rewardUSD = String(reward * entry.currencyPriceUSD);
      } else {
        entry.currencyPriceUSD = '1.00';
        entry.rewardUSD = reward;
      }
      entries.push(entry);
    }
  }
  return entries;
}

const getEntries = async () => {
  await initialize();
  const noContractEntries = await getNoContractEntries();
  const entriesV0 = await getEntriesPreV2(0);
  const entriesV1 = await getEntriesPreV2(1);
  const entriesV2 = await getEntriesV2();
  const entries = [
    ...noContractEntries,
    ...entriesV0,
    ...entriesV1,
    ...entriesV2,
  ];
  const csv = stringify(entries, { header: true });
  console.log();
  console.log(csv);
  const sortedEntries = entries.sort((a, b) => a.address - b.address);
  const sortedCSV = stringify(sortedEntries, { header: true, columns: [
    'address',
    'user',
    'event',
    'date',
    'reward',
    'currency',
    'currencyPriceUSD',
    'rewardUSD',
  ] });
  console.log(sortedCSV);
  const aggr = entries.reduce((aggr, entry) => {
    const { address, user, reward, currency, rewardUSD } = entry;
    if (!(address in aggr)) {
      aggr[address] = {
        address,
        user,
        events: 1,
        ETH: currency === 'ETH' ? Number(reward) : 0,
        xDAI: currency === 'xDAI' ? Number(reward) : 0,
        USD: Number(rewardUSD),
      };
    } else {
      aggr[address].events ++;
      aggr[address].USD += Number(rewardUSD);
      if (currency === 'ETH') {
        aggr[address].ETH += Number(reward);
      }
      else if (currency === 'xDAI') {
        aggr[address].xDAI += Number(reward);
      }
    }
    return aggr;
  }, {});
  const aggrValues = Object.values(aggr);
  const aggrCSV = stringify(aggrValues, { header: true });
  console.log(aggrCSV);
  if (Object.keys(ethPrices).length > Object.keys(savedETHPrices).length) {
    fs.writeFileSync(ethPricesPath, JSON.stringify(ethPrices));
  }
  dbModule.disconnect();
}

getEntries();
