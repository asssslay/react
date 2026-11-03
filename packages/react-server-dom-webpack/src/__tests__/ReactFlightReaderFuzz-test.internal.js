/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 * @emails react-core
 * @jest-environment node
 */

'use strict';

// Round-trip generated JavaScript values through the Flight server and client.
// Vary when Promises resolve, modules load, and stream items are produced.
// Check the results against the source values after each event.
// The reply host encodes on the client and decodes on the server instead.
//
// Set FUZZ_TEST_SEED to replay a run and FLIGHT_FUZZ_CASES to change its size.
// FLIGHT_FUZZ_SHRINK limits the number of replays used to reduce a failure.

const {createOracle} = require('./fuzz/FlightReaderOracle');
const {createFixture} = require('./fuzz/FlightReaderFuzzFixture');
const {
  createRandom,
  generateGraph,
  generateScaleGraph,
  shrinkGraph,
  formatGraphTest,
  formatReplayCommand,
} = require('./fuzz/FlightReaderFuzzGraph');

const SEED = process.env.FUZZ_TEST_SEED || 'default';
const caseCount = +(process.env.FLIGHT_FUZZ_CASES ?? 4);
const shrinkBudget = +(process.env.FLIGHT_FUZZ_SHRINK ?? 0);
if (!Number.isSafeInteger(caseCount) || caseCount < 1) {
  throw new Error('FLIGHT_FUZZ_CASES must be a positive integer.');
}
if (!Number.isSafeInteger(shrinkBudget) || shrinkBudget < 0) {
  throw new Error('FLIGHT_FUZZ_SHRINK must be a nonnegative replay budget.');
}
// Generated graphs perform real I/O and can exceed Jest's default timeout.
jest.setTimeout(shrinkBudget > 0 ? 120000 : 15000);
// Keep the first seed unchanged so the printed command replays that case.
const seeds = Array.from({length: caseCount}, (_, i) =>
  i === 0 ? SEED : SEED + ':' + i,
);

// Subscribe to the root Promise before the first packet, after the first
// flush, after all events, or after EOF.
const readTimings = ['early', 'late', 'after-events', 'after-eof'];

// Large graphs deliberately keep more than ten streams active at once.
// Adjust Node's listener warning for signals created by these tests only.
const NativeAbortController = global.AbortController;
class FuzzAbortController extends NativeAbortController {
  constructor() {
    super();
    require('node:events').setMaxListeners(Infinity, this.signal);
  }
}

describe.each(['node', 'edge', 'reply'])('ReactFlightReaderFuzz (%s)', host => {
  let React;
  let ReactServer;
  let ReactDOMServer;
  let ReactServerDOMServer;
  let ReactServerDOMClient;
  let WebpackMock;
  let assertConsoleErrorDev;
  let assertConsoleLogsCleared;
  let serverConfig;

  // Restore console methods between cases to avoid retaining old server modules.
  const consoleMethods = Object.fromEntries(
    Object.entries(Object.getOwnPropertyDescriptors(console)).filter(
      ([, descriptor]) => typeof descriptor.value === 'function',
    ),
  );

  // Run a pasted case at every read timing unless one was specified.
  async function runGraph({
    readTiming,
    seed = SEED,
    scheduleIndex = 0,
    ...graph
  }) {
    const timings = readTiming === undefined ? readTimings : [readTiming];
    for (let i = 0; i < timings.length; i++) {
      await runCaseWithReproduction({
        seed,
        readTiming: timings[i],
        scheduleIndex,
        graph,
      });
    }
  }

  async function runCaseWithReproduction(args) {
    const {seed, readTiming, graph} = args;
    const original = await runCase(args);
    if (original === null) {
      return;
    }
    let failure = original;
    let reduction = '';
    if (shrinkBudget > 0) {
      const signature = error => error.fuzzFailure || error.message;
      const reduced = await shrinkGraph(
        original.graph,
        async candidate => {
          let result;
          try {
            result = await runCase({...args, graph: candidate});
          } catch (error) {
            // A reduction can invalidate the graph, e.g. by replacing
            // an element that still has a props reference. Skip that reduction.
            return false;
          }
          return result !== null &&
            signature(result.error) === signature(original.error)
            ? result
            : false;
        },
        shrinkBudget,
      );
      if (reduced.reproduction !== null) {
        failure = reduced.reproduction;
      }
      reduction =
        '\nReduced from ' +
        Object.keys(graph.nodes).length +
        ' to ' +
        Object.keys(failure.graph.nodes).length +
        ' nodes in ' +
        reduced.attempts +
        ' replays. Each accepted reduction failed twice.\n';
    }
    const testName = expect.getState().currentTestName;
    const report =
      'Replay the original with:\n' +
      formatReplayCommand(seed, testName, __DEV__) +
      '\n' +
      reduction +
      '\nPaste this case next to the generated graphs and run with:\n' +
      formatReplayCommand(
        seed,
        'ReactFlightReaderFuzz (' + host + ') pasted',
        __DEV__,
      ) +
      '\n\n' +
      formatGraphTest(failure.graph, readTiming);
    throw new Error(report, {cause: failure.error});
  }

  async function runCase({seed, readTiming, scheduleIndex, graph}) {
    let replayGraph;
    try {
      await withFlightModules(async () => {
        const fixture = await createFixture(graph, {
          React,
          ReactServer,
          WebpackMock,
          createServerReference:
            host === 'reply'
              ? ReactServerDOMClient.createServerReference
              : undefined,
          observeProps(element, props) {
            oracle.observeProps(element, props);
          },
        });
        const oracle = createOracle(fixture, {render});
        const schedule = createSchedule(fixture, graph, seed, scheduleIndex);
        replayGraph = {
          ...graph,
          events: schedule.events.map(event => event.label),
          packetSizes: schedule.packetSizes,
          yieldBetweenPackets: schedule.yieldBetweenPackets,
        };
        await checkResponse(fixture, oracle, {...schedule, readTiming});
      });
      return null;
    } catch (error) {
      // Invalid graphs are rejected before attempting a replay.
      if (replayGraph === undefined) {
        throw error;
      }
      return {error, graph: replayGraph};
    }
  }

  function createSchedule(fixture, graph, seed, scheduleIndex) {
    const events = fixture.events.slice();
    const labels = new Set();
    events.forEach(event => {
      if (labels.has(event.label)) {
        throw new Error('Two events are labeled "' + event.label + '"');
      }
      labels.add(event.label);
    });
    // Change event order without changing the generated values.
    const order = createRandom(seed + '/order/' + scheduleIndex);
    if (graph.events !== undefined) {
      const position = label => {
        const index = graph.events.indexOf(label);
        return index === -1 ? graph.events.length : index;
      };
      events.sort((a, b) => position(a.label) - position(b.label));
    } else {
      shuffle(events, order);
    }
    // Other results must finish before this independent Promise resolves.
    events.splice(events.indexOf(fixture.unrelatedEvent), 1);
    events.push(fixture.unrelatedEvent);
    const packetSizes = [1 + order(8), 17 + order(100), 1024 + order(4096)];
    const yieldBetweenPackets = order(2) === 0;
    return {
      events,
      afterEof: graph.afterEof === true,
      packetSizes: graph.packetSizes || packetSizes,
      yieldBetweenPackets:
        graph.yieldBetweenPackets !== undefined
          ? graph.yieldBetweenPackets
          : yieldBetweenPackets,
    };
  }

  // Check progress after each event. Otherwise, a later event could hide
  // a missing wakeup.
  async function checkResponse(
    fixture,
    oracle,
    {readTiming, events, packetSizes, yieldBetweenPackets, afterEof = false},
    model = fixture.model,
  ) {
    const reported = [];
    if (host === 'reply' || readTiming === 'after-eof') {
      // Replies buffer writer output before decoding. The last read timing
      // also starts the Flight renderer with its events already released.
      for (let i = 0; i < events.length; i++) {
        if (!events[i].isClient) {
          releaseEvent(events[i]);
        }
      }
      events = events.filter(event => event.isClient);
    }

    const transport = startFlightResponse({
      ReactServerDOMServer,
      ReactServerDOMClient,
      WebpackMock,
      host,
      model,
      body:
        host === 'reply'
          ? await ReactServerDOMClient.encodeReply(model)
          : undefined,
      bufferedReply: host === 'reply' && readTiming === 'after-eof',
      packetSizes,
      yieldBetweenPackets,
      onError(error) {
        const digest = fixture.expectedErrors.has(error)
          ? fixture.valueMetadata.get(error).digest
          : undefined;
        if (digest === undefined) {
          throw error;
        }
        reported.push(error);
        return digest;
      },
    });
    try {
      const expectedModel =
        fixture.rootPromise === null ? fixture.model : fixture.resolvedModel;
      const root =
        fixture.rootPromise === null
          ? transport.root
          : Promise.resolve(transport.root);
      const checkProgress = async stage => {
        let pendingChecks = [];
        try {
          await transport.drain(
            () => {
              pendingChecks = oracle.getMissingResults();
              return pendingChecks.length === 0;
            },
            () =>
              stage +
              ': still waiting for ' +
              pendingChecks.map(entry => entry.path).join(', ') +
              '; not yet released: ' +
              fixture.events
                .filter(event => !event.released)
                .map(event => event.label)
                .join(', '),
          );
          await oracle.checkForMutations();
        } catch (error) {
          if (!error.message.startsWith('No progress')) {
            error.message = stage + ': ' + error.message;
          }
          throw error;
        }
      };
      if (readTiming === 'early') {
        oracle.subscribeToRoot(root, expectedModel, {subscribeAgain: true});
      }
      await transport.flush();
      if (readTiming === 'late') {
        oracle.subscribeToRoot(root, expectedModel, {subscribeAgain: true});
      }
      await checkProgress('after the first packets');
      const release = async event => {
        await runInIOCallback(() => releaseEvent(event));
        await checkProgress('after ' + event.label);
      };
      if (afterEof) {
        // Module loading can finish after EOF. Closing the transport must not
        // cancel those pending results.
        for (let i = 0; i < events.length; i++) {
          if (!events[i].isClient) {
            await release(events[i]);
          }
        }
        await transport.finish();
        transport.close();
        await transport.flush();
        await checkProgress('after EOF');
        events = events.filter(event => event.isClient);
      }
      for (let i = 0; i < events.length; i++) {
        await release(events[i]);
      }
      await transport.finish();
      if (readTiming === 'after-eof') {
        transport.close();
        await tick();
      }
      if (readTiming === 'after-events' || readTiming === 'after-eof') {
        oracle.subscribeToRoot(root, expectedModel, {subscribeAgain: true});
      }
      await checkProgress('after all events');
      await transport.drain(
        () => oracle.isComplete(),
        () => 'every outcome, render and stream item',
      );
      fixture.expectedErrors.forEach((thrower, error) => {
        if (!reported.includes(error)) {
          throw new Error(
            'The writer never reported "' +
              error.message +
              '", ' +
              thrower +
              '. Was it sent?',
          );
        }
      });
      await oracle.checkForMutations();
      // Read the completed response again to catch results that changed.
      oracle.subscribeToRoot(root, expectedModel);
      await transport.drain(
        () => oracle.isComplete(),
        () => 'reading the root again',
      );
      await oracle.checkForMutations();
    } finally {
      await transport.dispose();
    }
  }

  async function withFlightModules(run) {
    let failure = null;
    let loaded = false;
    serverConfig = null;
    const previousAbortController = global.AbortController;
    global.AbortController = FuzzAbortController;
    try {
      loadModules();
      loaded = true;
      await run();
    } catch (error) {
      failure = error;
    } finally {
      try {
        if (loaded) {
          // Check and clear console output before the next replay in both modes.
          const checks = [
            // Allow the warning on runtimes without ArrayBuffer.prototype.detached.
            () =>
              assertConsoleErrorDev(
                global.__loggedErrors
                  .map(entry => entry[0])
                  .filter(
                    message =>
                      typeof message === 'string' &&
                      message.includes('ArrayBuffer.prototype.detached'),
                  ),
                {withoutStack: true},
              ),
            assertConsoleLogsCleared,
          ];
          for (let i = 0; i < checks.length; i++) {
            try {
              checks[i]();
            } catch (error) {
              if (failure === null) {
                failure = error;
              }
            }
          }
        }
        // Node retains AsyncLocalStorage instances after jest.resetModules().
        serverConfig?.requestStorage?.disable();
        serverConfig?.componentStorage?.disable();
      } finally {
        Object.defineProperties(console, consoleMethods);
        global.AbortController = previousAbortController;
      }
    }
    if (failure !== null) {
      throw failure;
    }
  }

  // Reset caches so each case starts with unloaded client modules.
  function loadModules() {
    Object.defineProperties(console, consoleMethods);
    jest.resetModules();
    jest.useRealTimers();
    jest.mock('react', () => require('react/react.react-server'));
    const serverBuild = host === 'reply' ? 'node' : host;
    jest.doMock('react-server-dom-webpack/server', () =>
      require('react-server-dom-webpack/server.' + serverBuild),
    );
    WebpackMock = require('./utils/WebpackMock');
    ReactServerDOMServer = require(
      'react-server-dom-webpack/server.' + serverBuild,
    );
    serverConfig = require('react-server/src/ReactFlightServerConfig');
    ReactServer = require('react');
    jest.resetModules();
    __unmockReact();
    React = require('react');
    // The Edge client's Server Function references retain their metadata
    // when bound, so encodeReply can serialize them.
    ReactServerDOMClient = require(
      'react-server-dom-webpack/client.' + (host === 'reply' ? 'edge' : host),
    );
    ReactDOMServer = require('react-dom/server.edge');
    ({assertConsoleErrorDev} = require('internal-test-utils'));
    ({assertConsoleLogsCleared} = require('internal-test-utils/consoleMock'));
  }

  function render(element) {
    return ReactDOMServer.renderToReadableStream(element, {
      onError() {},
    }).then(stream => new Response(stream).text());
  }

  const formatCaseName = (label, seed, readTiming, scheduleIndex) =>
    label +
    ' seed=' +
    seed +
    ' readTiming=' +
    readTiming +
    ' schedule=' +
    scheduleIndex;

  // A reply cannot carry client references, elements, server components,
  // Errors or symbols; its loading leaves are Server Functions.
  const unsupportedReplyOperations = [
    'aggregate error',
    'fragment',
    'host',
    'symbol',
    'element',
    'props',
    'component',
    'failing component',
    'error',
    'rejected promise',
    'failing stream',
  ];

  describe('generated graphs', () => {
    seeds.forEach(seed => {
      for (let scheduleIndex = 0; scheduleIndex < 2; scheduleIndex++) {
        readTimings.forEach(readTiming => {
          // eslint-disable-next-line jest/valid-title
          it(formatCaseName('graph', seed, readTiming, scheduleIndex), () =>
            runGraph({
              ...generateGraph(seed, {
                without: host === 'reply' ? unsupportedReplyOperations : [],
              }),
              seed,
              readTiming,
              scheduleIndex,
            }),
          );
        });
      }
      const scaled = generateScaleGraph(seed);
      const readTiming =
        readTimings[createRandom(seed + '/scale-demand')(readTimings.length)];
      it(
        // eslint-disable-next-line jest/valid-title
        formatCaseName(
          scaled.shape + ' size=' + scaled.size,
          seed,
          readTiming,
          0,
        ),
        () => runGraph({...scaled.graph, seed, readTiming}),
      );
    });
  });

  // Paste a failing graph's test here.

  describe('known cases', () => {
    it('a bound argument first written inside a loading stream item', () =>
      runGraph({
        nodes: {
          ready: {module: 'loaded'},
          shared: {iterable: []},
          item: {object: {shared: 'shared', ready: 'ready'}},
          stream: {stream: ['item']},
          fn: {bound: ['shared']},
          root: {array: ['stream', 'fn']},
        },
        roots: {entry: 'root', other: 'root'},
        events: [
          'load fn',
          'stream item 0',
          'close stream',
          'start shared',
          'load ready',
        ],
        packetSizes: [1, 20, 4288],
        yieldBetweenPackets: false,
      }));

    it('shared object in two Promise results whose modules load and fail separately', () =>
      runGraph({
        nodes: {
          ready: {module: 'loaded'},
          failed: {module: 'failed', fail: 'load'},
          shared: {object: {}},
          loading: {array: ['shared', 'ready']},
          failing: {array: ['shared', 'failed']},
          first: {promise: 'loading'},
          second: {promise: 'failing'},
          last: {promise: 'shared'},
          roots: {array: ['first', 'second', 'last']},
        },
        roots: {entry: 'roots', other: 'roots'},
        events: [
          'resolve first to loading',
          'resolve second to failing',
          'resolve last to shared',
          'reject failed',
          'load ready',
        ],
      }));
    it.each(['', ':', '%3A', '%25:', ':%3A:', 'constructor', '01', '0', '🙂'])(
      'references through the property name %j',
      key =>
        runGraph({
          nodes: {
            ready: {module: 'export-0'},
            shared: {object: {ready: 'ready'}},
            object: {object: {[key]: 'shared', repeat: 'shared'}},
            later: {promise: 'shared'},
          },
          roots: {entry: 'object', other: 'later'},
        }),
    );

    it.each([':', '%3A:', ':%25:'])(
      'a cycle through the property name %j',
      key =>
        runGraph({
          nodes: {
            ready: {module: 'export-0'},
            cycle: {object: {[key]: 'cycle', value: 'ready'}},
            object: {object: {[key]: 'cycle'}},
          },
          roots: {entry: 'object', other: 'object'},
        }),
    );

    it('keeps undefined object properties', () =>
      runGraph({
        nodes: {
          missing: {special: 'undefined'},
          object: {object: {present: 'missing'}},
        },
        roots: {entry: 'object', other: 'object'},
      }));

    it('a large binary value written in two places', () =>
      runGraph({
        nodes: {
          binary: {special: 'large binary'},
          first: {array: ['binary']},
          second: {object: {again: 'binary'}},
          stream: {stream: ['first', 'second']},
        },
        roots: {entry: 'stream', other: 'stream'},
      }));

    it.each(['stream', 'bytes', 'iterable'])(
      '%s shared between two collections',
      type =>
        runGraph({
          nodes: {
            inner: {[type]: type === 'bytes' ? 2 : []},
            first: {set: ['inner']},
            second: {set: ['inner']},
          },
          roots: {entry: 'first', other: 'second'},
        }),
    );

    it('an iterator reused in later stream items', () =>
      runGraph({
        nodes: {
          value: {value: 'first'},
          iterator: {iterator: ['value']},
          first: {object: {iterator: 'iterator'}},
          second: {object: {iterator: 'iterator'}},
          stream: {stream: ['first', 'second']},
        },
        roots: {entry: 'stream', other: 'stream'},
      }));

    it('async iterator with cyclic items and a return value', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          item: {array: ['item', 'ready', 'iterator']},
          iterator: {
            iterable: ['item', 'item'],
            returns: 'item',
            single: true,
          },
        },
        roots: {entry: 'iterator', other: 'iterator'},
      }));

    it.each(['stream', 'bytes', 'iterable'])(
      '%s in repeated stream items',
      type =>
        runGraph({
          nodes: {
            inner: {[type]: type === 'bytes' ? 2 : []},
            item: {array: ['inner']},
            outer: {stream: ['item', 'item']},
          },
          roots: {entry: 'outer', other: 'outer'},
        }),
    );

    it.each(['stream', 'bytes', 'iterable'])(
      '%s shared between different stream items',
      type =>
        runGraph({
          nodes: {
            inner:
              type === 'bytes'
                ? {bytes: 2}
                : type === 'iterable'
                  ? {iterable: [], single: true}
                  : {stream: []},
            first: {array: ['inner']},
            second: {object: {again: 'inner'}},
            outer: {stream: ['first', 'second']},
          },
          roots: {entry: 'outer', other: 'outer'},
        }),
    );

    it('an async iterable returns a cycle that was already sent', () =>
      runGraph({
        nodes: {
          array: {array: ['object']},
          object: {object: {field0: 'array', field1: 'iterable'}},
          iterable: {iterable: [], returns: 'array'},
        },
        roots: {entry: 'array', other: 'array'},
      }));

    it('Set containing itself', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          set: {set: ['set', 'ready']},
        },
        roots: {entry: 'set', other: 'set'},
      }));

    it('Map containing itself', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          key: {value: 'value'},
          map: {
            map: [
              ['map', 'map'],
              ['key', 'ready'],
            ],
          },
        },
        roots: {entry: 'map', other: 'map'},
      }));

    it('two collections containing each other', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          key: {value: 'a'},
          a: {set: ['b', 'ready']},
          b: {map: [['key', 'a']]},
        },
        roots: {entry: 'a', other: 'b'},
      }));

    it('function bound to its array', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          fn: {bound: ['container']},
          container: {array: ['fn', 'container', 'ready']},
        },
        roots: {entry: 'container', other: 'container'},
      }));

    it('function bound to its Map', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          fn: {bound: ['container']},
          container: {
            map: [
              ['fn', 'ready'],
              ['container', 'container'],
            ],
          },
        },
        roots: {entry: 'container', other: 'container'},
      }));

    it('cycle first exposed through a bound function', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          fn: {bound: ['region']},
          region: {set: ['fn', 'region', 'ready']},
        },
        roots: {entry: 'fn', other: 'fn'},
      }));

    it('cycle first exposed through a Promise of a bound function', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          fn: {bound: ['region']},
          region: {object: {field0: 'fn', field1: 'region', field2: 'ready'}},
          exposure: {promise: 'fn'},
        },
        roots: {entry: 'exposure', other: 'exposure'},
      }));

    it('cycle first exposed through a stream item with a bound function', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          fn: {bound: ['region']},
          region: {array: ['fn', 'region', 'ready']},
          item: {array: ['fn']},
          exposure: {stream: ['item']},
        },
        roots: {entry: 'exposure', other: 'exposure'},
      }));

    it('async module failing to evaluate after loading', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0', async: true, fail: 'evaluate'},
          other: {module: 'export-1'},
          broken: {object: {field0: 'ready'}},
          fine: {object: {field0: 'other'}},
        },
        roots: {entry: 'broken', other: 'fine'},
      }));

    it('module failing to load after eof', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0', fail: 'load'},
          other: {module: 'export-1'},
          broken: {object: {field0: 'ready'}},
          fine: {object: {field0: 'other'}},
        },
        roots: {entry: 'broken', other: 'fine'},
        afterEof: true,
      }));

    it('streamed array containing itself', () =>
      runGraph({
        nodes: {
          item: {array: ['item']},
          stream: {stream: ['item']},
        },
        roots: {entry: 'stream', other: 'stream'},
      }));

    it('stream item with a failing module', () =>
      runGraph({
        nodes: {
          broken: {module: 'export-0', fail: 'load'},
          item: {array: ['broken']},
          stream: {stream: ['item']},
        },
        roots: {entry: 'stream', other: 'stream'},
      }));

    it('byte stream read with a BYOB reader', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          bytes: {bytes: 2},
          data: {object: {field0: 'ready'}},
        },
        roots: {entry: 'bytes', other: 'data'},
      }));

    it('byte stream closed while a read is waiting', () =>
      runGraph({
        nodes: {array: {array: ['bytes']}, bytes: {bytes: 3}},
        roots: {entry: 'array', other: 'array'},
        packetSizes: [21, 100000],
        yieldBetweenPackets: true,
      }));

    it('stream canceled while the writer is still producing', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          shared: {object: {field0: 'ready'}},
          stream: {stream: ['shared'], cancel: true},
        },
        roots: {entry: 'stream', other: 'shared'},
      }));

    it('cycle first exposed through two Promises', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          member: {object: {field0: 'ready', field1: 'set'}},
          set: {set: ['member']},
          first: {promise: 'member'},
          second: {promise: 'set'},
        },
        roots: {entry: 'first', other: 'second'},
      }));

    it('two stream items each waiting on a module', () =>
      runGraph({
        nodes: {
          first: {module: 'export-0'},
          second: {module: 'export-1'},
          item0: {set: ['first']},
          item1: {set: ['second']},
          stream: {stream: ['item0', 'item1']},
        },
        roots: {entry: 'stream', other: 'stream'},
      }));

    // Describing debug values must not consume their iterators.
    it('iterator resolved by a Promise', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          shared: {object: {field0: 'ready'}},
          iterator: {iterator: ['shared', 'ready']},
          later: {promise: 'iterator'},
        },
        roots: {entry: 'later', other: 'shared'},
      }));

    // Include enough values for the FormData reference ID to contain a hex letter.
    it('FormData bound to a function in a larger graph', () =>
      runGraph({
        nodes: {
          form: {special: 'form data'},
          set: {set: ['bytes', 'map', 'fn']},
          outerMap: {map: [['set', 'iterable']]},
          bytes: {bytes: 3},
          map: {map: []},
          fn: {bound: ['form']},
          iterable: {iterable: []},
        },
        roots: {entry: 'map', other: 'outerMap'},
        rootPromise: true,
      }));

    it('iterator over values still loading', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          shared: {object: {field0: 'ready'}},
          iterator: {iterator: ['shared', 'ready']},
          object: {object: {field0: 'shared', field1: 'iterator'}},
        },
        roots: {entry: 'object', other: 'object'},
      }));

    it('Blob beside a cycle', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0'},
          blob: {blob: 'blob'},
          array: {array: ['blob', 'ready', 'array']},
        },
        roots: {entry: 'array', other: 'blob'},
      }));

    it('module loading after eof', () =>
      runGraph({
        nodes: {
          ready: {module: 'export-0', async: true},
          region: {object: {field0: 'ready', field1: 'region'}},
        },
        roots: {entry: 'region', other: 'region'},
        afterEof: true,
      }));
  });

  // These value types are only supported in server-to-client responses.
  if (host !== 'reply') {
    describe('known cases with elements, components or Errors', () => {
      it('a Promise resolves before another value containing it finishes loading', () =>
        runGraph({
          nodes: {
            value: {value: null},
            data: {promise: 'value'},
            ready: {module: 'loaded'},
            section: {object: {data: 'data', ready: 'ready'}},
            later: {promise: 'section'},
            root: {object: {data: 'data', section: 'later'}},
          },
          roots: {entry: 'root', other: 'root'},
          events: [
            'resolve later to section',
            'resolve data to value',
            'load ready',
          ],
        }));

      it.each([
        ['stream', null],
        ['stream', 'keyed'],
        ['iterable', null],
      ])(
        'a shared %s first returned through element context with key %s',
        async (type, key) => {
          await withFlightModules(async () => {
            const fixture = await createFixture(
              {
                nodes: {
                  a: {value: 'a'},
                  b: {value: 'b'},
                  shared:
                    type === 'stream'
                      ? {stream: ['a', 'b']}
                      : {iterable: ['a', 'b'], single: true},
                },
                roots: {entry: 'shared', other: 'shared'},
              },
              {React, ReactServer, WebpackMock},
            );
            const shared = fixture.model.entry;
            function Body() {
              return shared;
            }
            // Return the stream through an element first, then as an object property.
            const first =
              type === 'stream'
                ? ReactServer.createElement(Body, {key})
                : ReactServer.createElement(ReactServer.Fragment, null, shared);
            const oracle = createOracle(fixture, {render});
            await checkResponse(
              fixture,
              oracle,
              {
                readTiming: 'early',
                events: fixture.events,
                packetSizes: [1, 21, 4096],
                yieldBetweenPackets: true,
              },
              {...fixture.model, entry: first},
            );
          });
        },
      );

      it('an element first written inside another loading value', () =>
        runGraph({
          nodes: {
            element: {host: 'atom'},
            ready: {module: 'loaded'},
            array: {array: ['element', 'ready']},
            first: {promise: 'array'},
            second: {promise: 'element'},
            root: {array: ['first', 'second']},
          },
          roots: {entry: 'root', other: 'root'},
          events: [
            'resolve first to array',
            'resolve second to element',
            'load ready',
          ],
        }));

      it('a Fragment first received inside a stream item', () =>
        runGraph({
          nodes: {
            fragment: {fragment: []},
            item: {array: ['fragment']},
            stream: {stream: ['item']},
          },
          roots: {entry: 'stream', other: 'stream'},
        }));

      it('an Error that is its own cause', () =>
        runGraph({
          nodes: {error: {error: 'self cause', cause: 'error'}},
          roots: {entry: 'error', other: 'error'},
        }));
      // The function and its bound container refer to each other. Both
      // Promises must resolve with complete values once the module loads.
      it('a repeated Server Function bound to its loading container', () =>
        runGraph({
          nodes: {
            ready: {module: 'loaded'},
            argument: {object: {value: 'ready', fn: 'fn'}},
            fn: {bound: ['argument']},
            first: {promise: 'argument'},
            result: {promise: 'fn'},
          },
          roots: {entry: 'first', other: 'result'},
          events: [
            'load fn',
            'resolve first to argument',
            'resolve result to fn',
            'load ready',
          ],
          packetSizes: [1, 25, 2091],
          yieldBetweenPackets: true,
        }));

      it('props object referenced again while a prop is loading', () =>
        runGraph({
          nodes: {
            payload: {module: 'export-0'},
            data: {object: {field0: 'props'}},
            value: {array: ['payload', 'data']},
            element: {element: 'value'},
            props: {props: 'element'},
          },
          roots: {entry: 'element', other: 'props'},
        }));

      it('props sub-object referenced while a sibling prop is loading', () =>
        runGraph({
          nodes: {
            payload: {module: 'export-0'},
            sibling: {module: 'export-1'},
            selected: {object: {field0: 'payload'}},
            value: {array: ['selected', 'sibling']},
            element: {element: 'value'},
          },
          roots: {entry: 'element', other: 'selected'},
        }));

      it('element reaching itself through its props', () =>
        runGraph({
          nodes: {
            payload: {module: 'export-0'},
            data: {object: {field0: 'element'}},
            value: {array: ['payload', 'data']},
            element: {element: 'value'},
          },
          roots: {entry: 'element', other: 'data'},
        }));

      it('a cyclic Map with values that load independently', () =>
        runGraph({
          nodes: {
            value1: {value: 'other value'},
            value2: {value: ''},
            value3: {value: 42},
            text0: {text: 'text-898'},
            blob0: {blob: 'blob-116'},
            host0: {host: 'atom'},
            module1: {module: 'export-1', async: true},
            array0: {array: ['array0']},
            set0: {set: ['map1']},
            map0: {
              map: [
                ['blob0', 'bytes0'],
                ['value3', 'props0'],
                ['module1', 'iterable0'],
                ['set0', 'value2'],
                ['array0', 'value1'],
              ],
            },
            map1: {
              map: [
                ['host0', 'host0'],
                ['element0', 'host0'],
                ['array0', 'host0'],
                ['blob0', 'text0'],
              ],
            },
            bytes0: {bytes: 2},
            element0: {element: 'host0'},
            props0: {props: 'element0'},
            iterable0: {iterable: []},
          },
          roots: {entry: 'map0', other: 'array0'},
          afterEof: true,
          rootPromise: true,
          events: [
            'bytes0 chunk 1',
            'load module1',
            'bytes0 chunk 0',
            'return iterable0',
            'load element0',
            'evaluate module1',
            'resolve the root Promise',
            'resolve an unrelated Promise',
          ],
          packetSizes: [3, 89, 2624],
          yieldBetweenPackets: false,
          readTiming: 'early',
        }));

      it('built-in values with a cycle and a loading module', () =>
        runGraph({
          nodes: {
            ready: {module: 'export-0'},
            nothing: {value: null},
            flag: {value: true},
            bigint: {special: 'bigint'},
            nan: {special: 'nan'},
            negativeZero: {special: 'negative zero'},
            infinity: {special: 'infinity'},
            symbol: {special: 'symbol'},
            escaped: {special: 'escaped'},
            text: {text: 'text'},
            date: {date: 1700000000000},
            buffer: {special: 'buffer'},
            binary: {binary: 3},
            typed: {special: 'typed array'},
            view: {special: 'data view'},
            bigints: {special: 'bigint array'},
            blob: {blob: 'blob'},
            form: {special: 'form data'},
            host: {host: 'atom'},
            atoms: {
              array: [
                'nothing',
                'flag',
                'bigint',
                'nan',
                'negativeZero',
                'infinity',
                'symbol',
                'escaped',
                'text',
                'date',
                'buffer',
                'binary',
                'typed',
                'view',
                'bigints',
                'blob',
                'form',
                'host',
                'ready',
                'cycle',
              ],
            },
            cycle: {object: {field0: 'atoms', field1: 'cycle'}},
          },
          roots: {entry: 'cycle', other: 'cycle'},
        }));

      it('server component rendering after the rest of the response', () =>
        runGraph({
          nodes: {
            ready: {module: 'export-0'},
            body: {value: 'body'},
            component: {component: 'body'},
            data: {object: {field0: 'ready'}},
          },
          roots: {entry: 'component', other: 'data'},
        }));

      it('failing server component', () =>
        runGraph({
          nodes: {
            ready: {module: 'export-0'},
            error: {error: 'component failed'},
            component: {component: 'error', throw: true},
            data: {object: {field0: 'ready'}},
          },
          roots: {entry: 'component', other: 'data'},
        }));

      it('suspended server component element used twice', () =>
        runGraph({
          nodes: {
            body: {value: 'shared'},
            component: {component: 'body'},
            pair: {array: ['component', 'component']},
          },
          roots: {entry: 'pair', other: 'pair'},
        }));

      it('server error through a rejected Promise', () =>
        runGraph({
          nodes: {
            ready: {module: 'export-0'},
            error: {error: 'expected failure'},
            failing: {promise: 'error', reject: true},
            fine: {object: {field0: 'ready'}},
          },
          roots: {entry: 'failing', other: 'fine'},
        }));

      it('source stream failure while another value is still loading', () =>
        runGraph({
          nodes: {
            error: {error: 'stream failed'},
            stream: {stream: [], fail: 'error'},
            ready: {module: 'loaded'},
            object: {object: {ready: 'ready'}},
            later: {promise: 'object'},
          },
          roots: {entry: 'stream', other: 'later'},
          events: ['resolve later to object', 'fail stream', 'load ready'],
        }));

      it.each(['cause', 'errors'])(
        'failing stream reachable only through Error.%s',
        property =>
          runGraph({
            nodes: {
              failure: {error: 'stream failed'},
              stream: {stream: [], fail: 'failure'},
              error: {
                error: 'outer',
                [property]: property === 'cause' ? 'stream' : ['stream'],
              },
            },
            roots: {entry: 'error', other: 'error'},
          }),
      );

      it('Error whose cause is still loading', () =>
        runGraph({
          nodes: {
            ready: {module: 'export-0'},
            error: {error: 'cause loading', cause: 'ready'},
            set: {set: ['error']},
          },
          roots: {entry: 'set', other: 'set'},
        }));

      it('Error with cyclic cause', () =>
        runGraph({
          nodes: {
            ready: {module: 'export-0'},
            cell: {object: {field0: 'ready', field1: 'error'}},
            error: {error: 'cyclic cause', cause: 'cell'},
          },
          roots: {entry: 'error', other: 'cell'},
        }));
    });
  }
});

const MAX_FLUSH_ATTEMPTS = 30;

function startFlightResponse({
  ReactServerDOMServer,
  ReactServerDOMClient,
  WebpackMock,
  host,
  model,
  body,
  bufferedReply,
  packetSizes,
  yieldBetweenPackets,
  onError,
}) {
  const manifest = {
    moduleMap: null,
    serverModuleMap: WebpackMock.webpackServerMap,
    moduleLoading: null,
  };
  const errors = [];
  let queued = [];
  let serverDone = false;
  let closed = false;
  let disposed = false;
  let packet = 0;
  let send;
  let end;
  let abort;
  let root;
  let serverCompletion;
  const options = {
    onError(error) {
      if (disposed) {
        return 'cleanup';
      }
      try {
        return onError(error);
      } catch (unexpected) {
        errors.push(unexpected);
        return 'unexpected';
      }
    },
  };
  if (host === 'reply') {
    serverDone = true;
    serverCompletion = Promise.resolve();
    abort = () => {};
    // The graph size is bounded by the generator. Test decoding independently
    // of the application's configurable limit on nested arrays.
    const replyOptions = {arraySizeLimit: Infinity};
    if (bufferedReply) {
      root = ReactServerDOMServer.decodeReply(
        body,
        WebpackMock.webpackServerMap,
        replyOptions,
      );
      send = () => {};
      end = () => {};
    } else {
      // Packet sizes count FormData entries, since the public decoder reads
      // entries from an async iterable rather than bytes from a stream.
      queued =
        typeof body === 'string' ? [['0', body]] : Array.from(body.entries());
      const waiting = [];
      const arrived = [];
      let ended = false;
      const deliver = () => {
        while (waiting.length && (arrived.length || ended)) {
          waiting.shift()(
            arrived.length
              ? {done: false, value: arrived.shift()}
              : {done: true, value: undefined},
          );
        }
      };
      const entries = {
        [Symbol.asyncIterator]() {
          return {
            next() {
              return new Promise(resolve => {
                waiting.push(resolve);
                deliver();
              });
            },
          };
        },
      };
      root = ReactServerDOMServer.decodeReplyFromAsyncIterable(
        entries,
        WebpackMock.webpackServerMap,
        replyOptions,
      );
      send = entry => {
        arrived.push(entry);
        deliver();
      };
      end = () => {
        ended = true;
        deliver();
      };
    }
  } else if (host === 'node') {
    const {PassThrough, Writable} = require('stream');
    const {finished} = require('stream/promises');
    const input = new PassThrough();
    root = ReactServerDOMClient.createFromNodeStream(input, manifest);
    send = bytes => input.write(bytes);
    end = () => input.end();
    const output = new Writable({
      write(bytes, encoding, done) {
        queued.push(Buffer.from(bytes));
        done();
      },
      final(done) {
        serverDone = true;
        done();
      },
    });
    serverCompletion = finished(output, {cleanup: true}).catch(error => {
      if (!disposed) {
        errors.push(error);
      }
    });
    const writer = ReactServerDOMServer.renderToPipeableStream(
      model,
      WebpackMock.webpackMap,
      options,
    );
    abort = reason => writer.abort(reason);
    writer.pipe(output);
  } else {
    let controller;
    const input = new ReadableStream({start: value => (controller = value)});
    root = ReactServerDOMClient.createFromReadableStream(input, {
      serverConsumerManifest: manifest,
    });
    send = bytes => controller.enqueue(bytes);
    end = () => controller.close();
    const control = new AbortController();
    const output = ReactServerDOMServer.renderToReadableStream(
      model,
      WebpackMock.webpackMap,
      {
        ...options,
        signal: control.signal,
      },
    );
    abort = reason => control.abort(reason);
    const reader = output.getReader();
    serverCompletion = (async () => {
      for (;;) {
        const {done, value} = await reader.read();
        if (done) {
          serverDone = true;
          return;
        }
        queued.push(Buffer.from(value));
      }
    })().catch(error => {
      if (!disposed) {
        errors.push(error);
      }
    });
  }

  // Split output into uneven packets to exercise partial reads.
  async function flush() {
    await tick();
    if (host === 'reply') {
      while (queued.length) {
        const count = packetSizes[packet++ % packetSizes.length];
        queued.splice(0, count).forEach(send);
        if (yieldBetweenPackets) {
          await tick();
        }
      }
    } else {
      // A broken byte-stream reader may transfer the packet's buffer. Give
      // the transport its own buffer so this can fail the case without also
      // detaching Node's shared Buffer pool and breaking Jest's file reads.
      const bytes = new Uint8Array(Buffer.concat(queued));
      queued = [];
      for (let offset = 0; offset < bytes.length; ) {
        const size = packetSizes[packet++ % packetSizes.length];
        send(bytes.subarray(offset, offset + size));
        offset += size;
        if (yieldBetweenPackets) {
          await tick();
        }
      }
    }

    await tick();
    if (errors.length) {
      throw errors[0];
    }
  }

  async function drain(predicate, describe) {
    for (let turn = 0; turn < MAX_FLUSH_ATTEMPTS; turn++) {
      await flush();
      if (predicate()) {
        return;
      }
      // Blob reads and Edge streams may need a timer or I/O callback.
      await new Promise(resolve => setTimeout(resolve, 1));
    }
    const error = new Error('No progress: ' + describe());
    error.fuzzFailure = 'liveness';
    throw error;
  }

  return {
    root,
    flush,
    drain,
    // Flush all server output without closing the client stream. Pending
    // values must complete without relying on EOF.
    finish: () =>
      drain(
        () => serverDone && queued.length === 0,
        () => 'writer completion',
      ),
    close() {
      if (!closed) {
        closed = true;
        end();
      }
    },
    async dispose() {
      disposed = true;
      try {
        if (!serverDone) {
          abort();
        }
        // Aborting can schedule more writer work. Wait for the output and its
        // read pump to finish before resetting modules for the next case.
        await serverCompletion;
      } finally {
        if (!closed) {
          closed = true;
          end();
        }
      }
      queued = [];
      await tick();
    },
  };
}

const tick = () => new Promise(resolve => setImmediate(resolve));

// MessageChannel makes React's async tracing treat this as I/O.
// Timers and microtasks don't exercise the same debug-info path.
function runInIOCallback(fn) {
  return new Promise((resolve, reject) => {
    const {MessageChannel} = require('node:worker_threads');
    const channel = new MessageChannel();
    channel.port1.onmessage = () => {
      channel.port1.close();
      try {
        resolve(fn());
      } catch (error) {
        reject(error);
      }
    };
    channel.port2.postMessage(undefined);
  });
}

function shuffle(array, pick) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = pick(i + 1);
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

function releaseEvent(event) {
  event.released = true;
  event.run();
}
