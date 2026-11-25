// NO_AUTO_TRANSPILE
import assert from 'assert';
import derive from '../../derive.js';
import Client from '../../../base/ws/Client.js';

// native ts test, intentionally not transpiled: pins the derive trades
// handler to the venue wire shape. the channel sends `params.data` as an
// array of trade objects, while a single-object frame is still accepted.
// treating the array as a dict iterated zero times, so watchTrades resolved
// an empty cache without error. Run: npx tsx ts/src/pro/test/base/test.handleTrade.derive.ts

const INSTRUMENT = {
    'instrument_type': 'perp',
    'instrument_name': 'BTC-PERP',
    'base_currency': 'BTC',
    'quote_currency': 'USDC',
    'is_active': true,
    'tick_size': '0.01',
    'minimum_amount': '1',
    'amount_step': '0.01',
    'maker_fee_rate': '0.0001',
    'taker_fee_rate': '0.0003',
};

function createHarness () {
    const exchange: any = new derive ({
        'enableRateLimit': false,
    });
    const market = exchange.parseMarket (INSTRUMENT);
    exchange.setMarkets ([ market ]);
    const url = exchange.urls['api']['ws'];
    const noop = () => {};
    const client: any = new Client (url, noop, noop, noop, noop, {});
    client.connect = () => {
        if (client.isConnected !== true) {
            client.isConnected = true;
            (client.connected as any).resolve (url);
        }
        return client.connected;
    };
    client.send = async (message: any) => message;
    exchange.clients = {};
    exchange.clients[url] = client;
    return { 'exchange': exchange, 'client': client, 'symbol': market['symbol'] };
}

function trade (id: string) {
    return {
        'trade_id': id,
        'instrument_name': 'BTC-PERP',
        'timestamp': 1790792657383,
        'trade_price': '83863',
        'trade_amount': '0.0357',
        'direction': 'buy',
        'liquidity_role': 'taker',
        'trade_fee': '0.44910870418',
    };
}

function tradeFrame (data: any) {
    return {
        'method': 'subscription',
        'params': {
            'channel': 'trades.BTC-PERP',
            'data': data,
        },
    };
}

async function testArrayPayload () {
    const harness = createHarness ();
    const exchange = harness['exchange'];
    const client = harness['client'];
    const symbol = harness['symbol'];
    const pending = exchange.watchTrades (symbol);
    exchange.handleMessage (client, tradeFrame ([ trade ('trade-1'), trade ('trade-2'), trade ('trade-3') ]));
    const trades = await pending;
    assert.equal (trades.length, 3, 'an array payload must produce one trade per element');
    assert.deepEqual (trades.map ((entry: any) => entry['id']), [ 'trade-1', 'trade-2', 'trade-3' ], 'array payload trade ids');
    assert.equal (exchange.trades[symbol].length, 3, 'the trade cache must hold every array element');
}

async function testSingleObjectPayload () {
    const harness = createHarness ();
    const exchange = harness['exchange'];
    const client = harness['client'];
    const symbol = harness['symbol'];
    const pending = exchange.watchTrades (symbol);
    exchange.handleMessage (client, tradeFrame (trade ('trade-single')));
    const trades = await pending;
    assert.equal (trades.length, 1, 'a single-object payload must produce one trade');
    assert.equal (trades[0]['id'], 'trade-single', 'single-object payload trade id');
    assert.equal (exchange.trades[symbol].length, 1, 'the trade cache must hold the single object');
}

async function testAbsentPayload () {
    const harness = createHarness ();
    const exchange = harness['exchange'];
    const client = harness['client'];
    const symbol = harness['symbol'];
    const pending = exchange.watchTrades (symbol);
    exchange.handleMessage (client, tradeFrame (undefined));
    const trades = await pending;
    assert.equal (trades.length, 0, 'an absent payload must resolve an empty trade list');
}

async function testDeriveHandleTrade () {
    await testArrayPayload ();
    await testSingleObjectPayload ();
    await testAbsentPayload ();
}

export default testDeriveHandleTrade;

const invokedDirectly = process.argv[1] !== undefined && process.argv[1].indexOf ('test.handleTrade.derive') !== -1;
if (invokedDirectly) {
    testDeriveHandleTrade ().then (() => {
        console.log ('test.handleTrade.derive passed');
    }).catch ((error) => {
        console.error (error);
        process.exit (1);
    });
}
