const WebSocket = require("ws");
const fs = require("fs");
const csv = require("fast-csv");

var sock = new WebSocket("wss://stream.binance.com/stream");
var outputStream = fs.createWriteStream(`./output${new Date()}.csv`);
// var jsonOutputStream = fs.createWriteStream(`./output${new Date()}.json`);
// jsonOutputStream.write('[');

const csvStream = csv.format({ headers: true });
csvStream.pipe(outputStream).on("end", () => {});

sock.on("open", (data) => {
  console.log("Connection Established");
  sock.send(
    JSON.stringify({
      method: "SUBSCRIBE",
      params: ["btcusdt@aggTrade", "btcusd@trade"],
      id: 1,
    })
  );
});

sock.on("error", (data) => {
  console.log("An error has occured");
  console.log(data);
});

sock.on("close", (data) => {
  // jsonOutputStream.write(']')
});

var datalist = [];

var drawData = () => {
  console.clear();
  console.table(
    datalist
      .filter((instance) => instance.e === "trade" || instance.e === "aggTrade")
      .map((instance) => ({
        Type: instance.e === "trade" ? "Trade" : "Aggregate Trade",
        "Event Time": new Date(instance.E),
        Symbol: instance.s,
        ID: instance.e === "trade" ? instance.t : instance.a,
        Price: instance.p,
        Quantity: instance.q,
        "First trade id": instance.f,
        "last trade id": instance.l,
        "Trade time": new Date(instance.E),
      }))
  );
};

var writeData = (instance) => {
  csvStream.write({
    Type: instance.e === "trade" ? "Trade" : "Aggregate Trade",
    "Event Time": new Date(instance.E),
    Symbol: instance.s,
    ID: instance.e === "trade" ? instance.t : instance.a,
    Price: instance.p,
    Quantity: instance.q,
    "First trade id": instance.f,
    "last trade id": instance.l,
    "Trade time": new Date(instance.E),
  });
};

sock.on("message", (raw_data) => {
  try {
    let data = JSON.parse(raw_data).data;

    if (data) datalist.push(data);
    if (data) writeData(data);

    drawData();
  } catch (e) {
    // console.log(raw_data);
  }
});
