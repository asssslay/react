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

// These tests exercise the oracle directly, without running React.
const {createOracle} = require('./fuzz/FlightReaderOracle');

const tick = () => new Promise(resolve => setImmediate(resolve));

function createTestOracle(
  source,
  {
    valueMetadata = new Map(),
    requiredEvents = new Map(),
    bytes = new Map(),
    render = () => {},
  } = {},
) {
  const oracle = createOracle(
    {
      model: source,
      resolvedModel: null,
      rootPromise: null,
      valueMetadata,
      requiredEvents,
      bytes,
    },
    {render},
  );
  let resolve;
  let reject;
  // Tests control delivery independently of the source's events.
  oracle.subscribeToRoot(
    {
      then(onFulfill, onReject) {
        resolve = onFulfill;
        reject = onReject;
      },
    },
    source,
  );
  return {oracle, resolve, reject};
}

describe('FlightReaderOracle', () => {
  beforeEach(() => {
    jest.useRealTimers();
  });

  it.each([
    ['negative zero', -0, -0, 0],
    ['NaN', NaN, NaN, null],
    ['object keys', {value: undefined}, {value: undefined}, {}],
    ['array order', [1, 2], [1, 2], [2, 1]],
    ['array length', [undefined], [undefined], []],
    ['Set entries', new Set([1, 2]), new Set([1, 2]), new Set([1, 3])],
    ['Map keys', new Map([[1, 2]]), new Map([[1, 2]]), new Map([[2, 2]])],
    ['Map values', new Map([[1, 2]]), new Map([[1, 2]]), new Map([[1, 3]])],
    ['Date value', new Date(1), new Date(1), new Date(2)],
  ])('compares %s', (name, source, correct, incorrect) => {
    const valid = createTestOracle(source);
    valid.resolve(correct);
    expect(valid.oracle.isComplete()).toBe(true);

    const invalid = createTestOracle(source);
    invalid.resolve(incorrect);
    expect(() => invalid.oracle.throwIfFailed()).toThrow();
  });

  it('checks every received copy along a cycle, including after delivery', async () => {
    const source = {value: 'expected'};
    source.next = source;
    const copy = {value: 'expected'};
    copy.next = copy;
    const received = {value: 'expected', next: copy};
    const {oracle, resolve} = createTestOracle(source);
    resolve(received);
    expect(oracle.isComplete()).toBe(true);
    await expect(oracle.checkForMutations()).resolves.toBeUndefined();

    copy.value = 'changed';
    await expect(oracle.checkForMutations()).rejects.toThrow('.next.value');
    const invalid = createTestOracle(source);
    invalid.resolve(received);
    expect(() => invalid.oracle.throwIfFailed()).toThrow('root.next.value');
  });

  it('rejects merging distinct source objects with equal contents', () => {
    const source = {a: {value: 1}, b: {value: 1}};
    const valid = createTestOracle(source);
    valid.resolve({a: {value: 1}, b: {value: 1}});
    expect(valid.oracle.isComplete()).toBe(true);

    const merged = {value: 1};
    const invalid = createTestOracle(source);
    invalid.resolve({a: merged, b: merged});
    expect(() => invalid.oracle.throwIfFailed()).toThrow(
      'already delivered for a different value',
    );
  });

  it('compares the type and saved bytes of a detached source view', () => {
    const source = new Uint8Array([0, 1, 2, 3]).subarray(1, 3);
    const options = {bytes: new Map([[source, [1, 2]]])};
    structuredClone(source.buffer, {transfer: [source.buffer]});
    expect(source.byteLength).toBe(0);

    const valid = createTestOracle(source, options);
    valid.resolve(new Uint8Array([9, 1, 2, 9]).subarray(1, 3));
    expect(valid.oracle.isComplete()).toBe(true);

    [new Uint8Array([1, 3]), new Int8Array([1, 2])].forEach(incorrect => {
      const invalid = createTestOracle(source, options);
      invalid.resolve(incorrect);
      expect(() => invalid.oracle.throwIfFailed()).toThrow();
    });
  });

  it('requires delivery only after all inputs arrive', () => {
    const first = {};
    const second = {};
    const firstLoad = {released: false};
    const secondLoad = {released: false};
    const source = {first, second};
    const options = {
      valueMetadata: new Map([
        [first, {type: 'module', value: 'first'}],
        [second, {type: 'module', value: 'second'}],
      ]),
      requiredEvents: new Map([
        [first, [firstLoad]],
        [second, [secondLoad]],
      ]),
    };
    const {oracle, resolve} = createTestOracle(source, options);
    expect(oracle.getMissingResults()).toEqual([]);
    expect(oracle.isComplete()).toBe(false);
    firstLoad.released = true;
    expect(oracle.getMissingResults()).toEqual([]);

    const early = createTestOracle(source, options);
    early.resolve({first: 'first', second: 'second'});
    expect(() => early.oracle.throwIfFailed()).toThrow(
      'Delivered before its inputs arrived',
    );

    const rejected = createTestOracle(source, options);
    const reason = new TypeError('decoding failed');
    rejected.reject(reason);
    expect(() => rejected.oracle.throwIfFailed()).toThrow(
      'unexpected rejection: TypeError: decoding failed',
    );
    expect(() => rejected.oracle.throwIfFailed()).toThrow(
      expect.objectContaining({cause: reason}),
    );

    secondLoad.released = true;
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root',
    ]);
    resolve({first: 'first', second: 'second'});
    expect(oracle.isComplete()).toBe(true);
  });

  it('requires the actual input error without waiting for other inputs', () => {
    const first = {};
    const second = {};
    const error = new Error('load failed');
    const source = {first, second};
    const options = {
      valueMetadata: new Map([
        [first, {type: 'module', value: 'first'}],
        [second, {type: 'module', value: 'second'}],
      ]),
      requiredEvents: new Map([
        [first, [{released: true, error}]],
        [second, [{released: false}]],
      ]),
    };
    const {oracle, reject} = createTestOracle(source, options);
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root',
    ]);
    reject(error);
    expect(oracle.isComplete()).toBe(true);

    const wrongError = createTestOracle(source, options);
    wrongError.reject(new Error('load failed'));
    expect(() => wrongError.oracle.throwIfFailed()).toThrow('root: rejected');

    const fulfilled = createTestOracle(source, options);
    fulfilled.resolve({first: 'first', second: 'second'});
    expect(() => fulfilled.oracle.throwIfFailed()).toThrow(
      'expected a rejection',
    );
  });

  it('distinguishes rejection with undefined from fulfillment', () => {
    const valid = createTestOracle(undefined);
    valid.resolve(undefined);
    expect(valid.oracle.isComplete()).toBe(true);

    const invalid = createTestOracle(undefined);
    invalid.reject(undefined);
    expect(() => invalid.oracle.throwIfFailed()).toThrow(
      'unexpected rejection: undefined',
    );
  });

  it('rejects a second call to a settlement callback', () => {
    const {oracle, resolve} = createTestOracle('value');
    resolve('value');
    expect(oracle.isComplete()).toBe(true);
    resolve('value');
    expect(() => oracle.throwIfFailed()).toThrow();
  });

  it('requires repeated subscriptions to receive the same object', () => {
    const source = {value: 'expected'};
    let result = {value: 'expected'};
    const thenable = {then: onFulfill => onFulfill(result)};
    const {oracle, resolve} = createTestOracle(source);
    resolve(result);
    oracle.subscribeToRoot(thenable, source, {subscribeAgain: true});
    expect(oracle.isComplete()).toBe(true);

    result = {value: 'expected'};
    oracle.subscribeToRoot(thenable, source);
    expect(() => oracle.throwIfFailed()).toThrow('changed its settlement');
  });

  it('requires a Promise result when ready and rechecks it after delivery', async () => {
    const source = {};
    const metadata = {
      type: 'promise',
      state: 'pending',
      value: {value: 'expected'},
    };
    const {oracle, resolve} = createTestOracle(source, {
      valueMetadata: new Map([[source, metadata]]),
    });
    let resolvePromise;
    resolve(new Promise(done => (resolvePromise = done)));
    expect(oracle.getMissingResults()).toEqual([]);
    expect(oracle.isComplete()).toBe(false);

    metadata.state = 'fulfilled';
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root.then',
    ]);
    const received = {value: 'expected'};
    resolvePromise(received);
    await tick();
    expect(oracle.isComplete()).toBe(true);

    received.value = 'changed';
    await expect(oracle.checkForMutations()).rejects.toThrow('root.then');
  });

  it('rejects replacing an observed Promise without subscribing to the replacement', async () => {
    const promise = {};
    const {oracle, resolve} = createTestOracle(
      {promise},
      {
        valueMetadata: new Map([
          [promise, {type: 'promise', state: 'fulfilled', value: 'value'}],
        ]),
      },
    );
    const received = {promise: Promise.resolve('value')};
    resolve(received);
    await tick();
    expect(oracle.isComplete()).toBe(true);
    await expect(oracle.checkForMutations()).resolves.toBeUndefined();

    received.promise = Promise.resolve('value');
    const then = jest.spyOn(received.promise, 'then');
    await expect(oracle.checkForMutations()).rejects.toThrow('was replaced');
    expect(then).not.toHaveBeenCalled();
  });

  it.each(['stream', 'iterable'])('compares %s items in order', async type => {
    const source = {};
    const options = {
      valueMetadata: new Map([
        [
          source,
          {
            type,
            items: ['first', 'second'],
            events: Array.from({length: 3}, () => ({released: true})),
            returnValue: undefined,
          },
        ],
      ]),
    };
    function createReceived(items) {
      if (type === 'stream') {
        return new ReadableStream({
          start(controller) {
            items.forEach(item => controller.enqueue(item));
            controller.close();
          },
        });
      }
      return {
        async *[Symbol.asyncIterator]() {
          yield* items;
        },
      };
    }
    const valid = createTestOracle(source, options);
    valid.resolve(createReceived(['first', 'second']));
    await tick();
    expect(valid.oracle.isComplete()).toBe(true);

    const invalid = createTestOracle(source, options);
    invalid.resolve(createReceived(['second', 'first']));
    await tick();
    expect(() => invalid.oracle.throwIfFailed()).toThrow(
      type === 'stream' ? 'root.item0' : 'root.next0',
    );
  });

  it('does not accept errors from unproduced repeated stream items', async () => {
    const source = {};
    const shared = {};
    const currentModule = {};
    const futureModule = {};
    const currentError = new Error('current item failed');
    const futureError = new Error('future item failed');
    const options = {
      valueMetadata: new Map([
        [
          source,
          {
            type: 'stream',
            // Only the first two items have been produced. The later copy of
            // shared must not make the future model's error observable yet.
            items: [shared, currentModule, shared, {shared, futureModule}],
            events: [true, true, false, false, false].map(released => ({
              released,
            })),
          },
        ],
        [currentModule, {type: 'module', value: 'current'}],
        [futureModule, {type: 'module', value: 'future'}],
      ]),
      requiredEvents: new Map([
        [currentModule, [{released: true, error: currentError}]],
        [futureModule, [{released: true, error: futureError}]],
      ]),
    };
    for (let i = 0; i < 2; i++) {
      const error = i === 0 ? currentError : futureError;
      const {oracle, resolve} = createTestOracle(source, options);
      let first = true;
      resolve({
        getReader: () => ({
          async read() {
            if (first) {
              first = false;
              return {done: false, value: {}};
            }
            throw error;
          },
        }),
      });
      await tick();
      if (error === currentError) {
        expect(oracle.isComplete()).toBe(true);
      } else {
        expect(() => oracle.throwIfFailed()).toThrow(
          'expected "current item failed"',
        );
      }
    }
  });

  it('requires an independent iterable item while an earlier item is blocked', async () => {
    const source = {};
    const module = {};
    const load = {released: false};
    const {oracle, resolve} = createTestOracle(source, {
      valueMetadata: new Map([
        [
          source,
          {
            type: 'iterable',
            items: [module, 'ready'],
            events: Array.from({length: 3}, () => ({released: true})),
            returnValue: 'end',
            single: true,
          },
        ],
        [module, {type: 'module', value: 'loaded'}],
      ]),
      requiredEvents: new Map([[module, [load]]]),
    });
    let resolveFirst;
    let resolveSecond;
    const results = [
      new Promise(done => (resolveFirst = done)),
      new Promise(done => (resolveSecond = done)),
      Promise.resolve({done: true, value: 'end'}),
    ];
    let index = 0;
    resolve({
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        return (
          results[index++] || Promise.resolve({done: true, value: undefined})
        );
      },
    });
    await tick();
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root.ahead.next1',
    ]);

    resolveSecond({done: false, value: 'ready'});
    await tick();
    expect(oracle.getMissingResults()).toEqual([]);
    expect(oracle.isComplete()).toBe(false);

    load.released = true;
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root.ahead.next0',
    ]);
    resolveFirst({done: false, value: 'loaded'});
    await tick();
    expect(oracle.isComplete()).toBe(true);
  });

  it('requires a stream error without rejecting the Promise that delivered the stream', async () => {
    const source = {};
    const stream = {};
    const failure = new Error('source failed');
    const failed = {released: true};
    const {oracle, resolve} = createTestOracle(source, {
      valueMetadata: new Map([
        [source, {type: 'promise', state: 'fulfilled', value: stream}],
        [stream, {type: 'stream', items: [], events: [failed], failure}],
        [failure, {type: 'error', digest: 'stream failure'}],
      ]),
    });
    let controller;
    resolve(
      Promise.resolve(new ReadableStream({start: c => (controller = c)})),
    );
    await tick();
    expect(oracle.isComplete()).toBe(false);
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root.then.end',
    ]);
    // The client receives the digest, not the original server Error.
    controller.error(
      Object.assign(new Error('redacted'), {digest: 'stream failure'}),
    );
    await tick();
    expect(oracle.isComplete()).toBe(true);
  });

  it('requires available bytes without waiting for the stream to close', async () => {
    const source = {};
    const produced = {released: false};
    const closed = {released: false};
    const {oracle, resolve} = createTestOracle(source, {
      valueMetadata: new Map([
        [
          source,
          {
            type: 'bytes',
            chunks: [new Uint8Array([1])],
            events: [produced, closed],
          },
        ],
      ]),
    });
    resolve({getReader: () => ({read: () => new Promise(() => {})})});
    await tick();
    expect(oracle.getMissingResults()).toEqual([]);
    produced.released = true;
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root.byte0',
    ]);
  });

  it('rechecks delivered iterator items without consuming the iterator again', async () => {
    const source = {};
    const item = {value: 'expected'};
    const received = [item].values();
    const next = jest.spyOn(received, 'next');
    const {oracle, resolve} = createTestOracle(source, {
      valueMetadata: new Map([
        [source, {type: 'iterator', items: [{value: 'expected'}]}],
      ]),
    });
    resolve(received);
    expect(oracle.isComplete()).toBe(true);
    await expect(oracle.checkForMutations()).resolves.toBeUndefined();
    expect(next).toHaveBeenCalledTimes(2);

    item.value = 'changed';
    await expect(oracle.checkForMutations()).rejects.toThrow('.item0.value');
    expect(next).toHaveBeenCalledTimes(2);
  });

  it('calls Server Functions again to check that their bound arguments are stable', async () => {
    const source = () => {};
    const {oracle, resolve} = createTestOracle(source, {
      valueMetadata: new Map([
        [
          source,
          {
            type: 'function',
            bound: ['bound'],
            args: ['argument'],
            result: ['bound', 'argument'],
          },
        ],
      ]),
    });
    let bound = 'bound';
    resolve(argument => [bound, argument]);
    expect(oracle.isComplete()).toBe(true);
    await expect(oracle.checkForMutations()).resolves.toBeUndefined();

    bound = 'changed';
    await expect(oracle.checkForMutations()).rejects.toThrow('changed');
  });

  it('waits for Blob contents before completing a mutation check', async () => {
    const source = new Blob([new Uint8Array([1])]);
    const received = new Blob([new Uint8Array([1])]);
    const {oracle, resolve} = createTestOracle(source);
    resolve(received);
    await tick();
    expect(oracle.isComplete()).toBe(true);

    let resolveRead;
    received.arrayBuffer = () => new Promise(done => (resolveRead = done));
    const recheck = oracle.checkForMutations();
    await tick();
    expect(oracle.isComplete()).toBe(false);
    resolveRead(new Uint8Array([2]).buffer);
    await expect(recheck).rejects.toThrow();
  });

  it('checks prop readiness even when an unloaded module exports undefined', () => {
    const module = {};
    const element = {type: {name: 'Component'}, props: {value: module}};
    const load = {released: false};
    const options = {
      valueMetadata: new Map([
        [element, {type: 'element', html: ''}],
        [module, {type: 'module', value: undefined}],
      ]),
      requiredEvents: new Map([[module, [load]]]),
    };
    const early = createTestOracle(element, options);
    early.oracle.observeProps(element, {value: undefined});
    expect(() => early.oracle.throwIfFailed()).toThrow(
      'before their inputs arrived',
    );

    load.released = true;
    const valid = createTestOracle(element, options);
    valid.oracle.observeProps(element, {value: undefined});
    valid.oracle.throwIfFailed();
  });

  it('keeps a shared value blocked while one possible containing model is loading', () => {
    const shared = {};
    const ready = {};
    const failed = {};
    const loading = {};
    const failing = {};
    const last = {};
    const load = {released: false};
    const failure = {released: true, error: new Error('module failed')};
    const {oracle, resolve} = createTestOracle([loading, failing, last], {
      valueMetadata: new Map([
        [
          loading,
          {type: 'promise', state: 'fulfilled', value: {shared, ready}},
        ],
        [
          failing,
          {type: 'promise', state: 'fulfilled', value: {shared, failed}},
        ],
        [last, {type: 'promise', state: 'fulfilled', value: shared}],
        [ready, {type: 'module', value: 'ready'}],
        [failed, {type: 'module', value: 'failed'}],
      ]),
      requiredEvents: new Map([
        [ready, [load]],
        [failed, [failure]],
      ]),
    });
    resolve([{then() {}}, {then() {}}, {then() {}}]);
    // Only the failing model must settle. shared could have been serialized
    // in the loading model, so the last Promise may still wait for it.
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root[1].then',
    ]);
    load.released = true;
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root[0].then',
      'root[1].then',
      'root[2].then',
    ]);
  });

  it('waits for a Server Component to render before requiring an error from its output', async () => {
    const source = {props: {}};
    const module = {};
    const error = new Error('output module failed');
    const renderEvent = {released: false};
    const renderedElement = {props: {value: module}};
    let rejectRender;
    const {oracle, resolve} = createTestOracle(source, {
      valueMetadata: new Map([
        [source, {type: 'element', renderedElement, renderEvent}],
        [module, {type: 'module', value: 'loaded'}],
      ]),
      requiredEvents: new Map([
        [source, [renderEvent]],
        [module, [{released: true, error}]],
      ]),
      render: () =>
        new Promise((onFulfill, onReject) => (rejectRender = onReject)),
    });
    resolve({});
    await tick();
    expect(oracle.getMissingResults()).toEqual([]);
    renderEvent.released = true;
    expect(oracle.getMissingResults().map(check => check.path)).toEqual([
      'root.render',
    ]);
    rejectRender(error);
    await tick();
    expect(oracle.isComplete()).toBe(true);
  });
});
