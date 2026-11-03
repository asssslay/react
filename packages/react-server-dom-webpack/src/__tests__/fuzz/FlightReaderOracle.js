/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

'use strict';

// Observe the client response using the fixture's expected values and events.
// The runner flushes each event before checking for missing results.
function createOracle(fixture, {render}) {
  const state = {
    fixture,
    render,
    resultChecks: [],
    failures: [],
    receivedValues: [],
    expectedByReceivedValue: new WeakMap(),
    observedAsyncValues: new WeakSet(),
    thenableResults: new WeakMap(),
    iteratorItems: new WeakMap(),
    functionCalls: new Map(),
    pendingTasks: 0,
  };
  return {
    subscribeToRoot(root, expectedValue, {subscribeAgain = false} = {}) {
      observeRoot(state, root, expectedValue, subscribeAgain);
    },
    observeProps(element, received) {
      observeProps(state, element, received);
    },
    throwIfFailed() {
      throwIfFailed(state);
    },
    getMissingResults() {
      throwIfFailed(state);
      const findModels = createModelLookup(fixture);
      return state.resultChecks.filter(
        check => !check.done && check.getExpectation(findModels).mustSettle,
      );
    },
    isComplete() {
      throwIfFailed(state);
      return (
        state.pendingTasks === 0 &&
        state.resultChecks.every(check => check.done)
      );
    },
    checkForMutations() {
      return recheckValues(state);
    },
  };
}

function getResultExpectation(value, inputsReady, errors, models) {
  return {
    value,
    canFulfill: inputsReady && errors.length === 0,
    errors: (inputsReady ? errors : []).concat(getModelErrors(models)),
    // A direct error must reject even if a referenced model is still blocked.
    mustSettle:
      inputsReady &&
      (errors.length > 0 || models.every(model => canSettle(model.events))),
  };
}

function registerResultCheck(state, expectedValue, path, getExpectation) {
  const check = {expectedValue, path, getExpectation, done: false};
  state.resultChecks.push(check);
  return check;
}

function handleFulfillment(state, check, received) {
  const {fixture} = state;
  guard(state, () => {
    expect(check.done).toBe(false);
    const expectation = check.getExpectation(createModelLookup(fixture));
    if (!expectation.canFulfill) {
      if (expectation.mustSettle) {
        throw new Error(check.path + ': expected a rejection');
      }
      throw new Error(describeEarlyResult(state, check));
    }
    observeValue(state, expectation.value, received, check.path);
    check.done = true;
  });
}

function handleRejection(state, check, error) {
  const {fixture} = state;
  guard(state, () => {
    expect(check.done).toBe(false);
    const expectation = check.getExpectation(createModelLookup(fixture));
    if (!isExpectedError(error, expectation.errors)) {
      if (!expectation.canFulfill && !expectation.mustSettle) {
        throw new Error(
          describeEarlyResult(state, check) +
            '; unexpected rejection: ' +
            String(error),
          {cause: error},
        );
      }
      if (expectation.errors.length === 0) {
        throw new Error(
          check.path + ': unexpected rejection: ' + String(error),
        );
      }
      assertExpectedError(error, expectation.errors, check.path);
    }
    check.done = true;
  });
}

function observeRoot(state, root, expectedValue, subscribeAgain) {
  const {fixture} = state;
  function getExpectation(findModels) {
    const events = getRequiredEvents(fixture, expectedValue);
    const rootPromise = fixture.rootPromise;
    const inputsReady =
      (rootPromise === null ||
        fixture.valueMetadata.get(rootPromise).state !== 'pending') &&
      canSettle(events);
    return getResultExpectation(
      expectedValue,
      inputsReady,
      getReleasedErrors(events),
      findModels(expectedValue),
    );
  }
  const check = registerResultCheck(
    state,
    expectedValue,
    'root',
    getExpectation,
  );
  subscribe(state, root, check, subscribeAgain);
}

function observeProps(state, element, received) {
  guard(state, () => {
    const events = getRequiredEvents(state.fixture, element.props);
    if (!areAllEventsReleased(events) || getReleasedErrors(events).length > 0) {
      throw new Error('A component received props before their inputs arrived');
    }
    observeValue(
      state,
      element.props,
      received,
      'props of ' + element.type.name,
    );
  });
}

function observeValue(state, expectedValue, received, path) {
  const root = {expectedValue, received, path};
  const todo = [root];
  const visited = new Set();
  // Keep function results and iterator items in this traversal so they can
  // refer back to the value that contains them.
  while (todo.length > 0) {
    const pair = todo.pop();
    try {
      const children = compareValue(state, pair, visited);
      if (children === null) {
        continue;
      }
      observeSpecialValue(state, pair);
      children.push(...getObservedChildren(state, pair));
      for (let i = children.length - 1; i >= 0; i--) {
        todo.push(children[i]);
      }
    } catch (error) {
      throwValueError(error, pair.path);
    }
  }
  state.receivedValues.push(root);
}

function observeSpecialValue(state, {expectedValue, received, path}) {
  const metadata = state.fixture.valueMetadata.get(expectedValue);
  if (metadata === undefined) {
    if (expectedValue instanceof Blob) {
      observeBlob(state, expectedValue, received);
    }
    return;
  }
  switch (metadata.type) {
    case 'module':
    case 'error':
      return;
    case 'function':
      observeServerFunction(state, received, metadata.args);
      return;
    case 'iterator':
      observeIterator(state, metadata, received);
      return;
  }
  if (state.observedAsyncValues.has(received)) {
    return;
  }
  state.observedAsyncValues.add(received);
  switch (metadata.type) {
    case 'promise':
      observePromise(state, metadata, expectedValue, received, path);
      return;
    case 'element':
      observeElement(state, metadata, expectedValue, received, path);
      return;
    case 'stream':
      observeStream(state, metadata, expectedValue, received, path);
      return;
    case 'iterable':
      observeAsyncIterable(state, metadata, received, path);
      return;
    case 'bytes':
      observeByteStream(state, metadata, expectedValue, received, path);
      return;
    case 'canceled':
      observeCanceledStream(state, metadata, received, path);
      return;
    default:
      throw new Error('Unknown value type: ' + metadata.type);
  }
}

function observePromise(state, metadata, expectedValue, received, path) {
  const {fixture} = state;
  function getExpectation(findModels) {
    const valueMetadata = fixture.valueMetadata.get(metadata.value);
    const events = getRequiredEvents(fixture, metadata.value);
    if (
      valueMetadata !== undefined &&
      valueMetadata.renderEvent !== undefined
    ) {
      // A Promise resolving to <ServerComponent /> waits for its render.
      events.push(valueMetadata.renderEvent);
    }
    const inputsReady =
      metadata.state === 'rejected' ||
      (metadata.state === 'fulfilled' && canSettle(events));
    let errors;
    if (metadata.state === 'rejected') {
      errors = [fixture.valueMetadata.get(metadata.reason)];
    } else if (
      valueMetadata !== undefined &&
      valueMetadata.type === 'element' &&
      valueMetadata.failure !== undefined
    ) {
      errors = [fixture.valueMetadata.get(valueMetadata.failure)];
    } else {
      errors = getReleasedErrors(events);
    }
    return getResultExpectation(
      metadata.value,
      inputsReady,
      errors,
      findModels(metadata.value),
    );
  }
  const check = registerResultCheck(
    state,
    expectedValue,
    path + '.then',
    getExpectation,
  );
  subscribe(state, received, check, true);
}

function observeElement(state, metadata, expectedValue, received, path) {
  const {fixture, render} = state;
  function getExpectation(findModels) {
    const events = (fixture.requiredEvents.get(expectedValue) || []).concat(
      getRequiredEvents(fixture, expectedValue.props),
      metadata.renderedElement === undefined
        ? []
        : getRequiredEvents(fixture, metadata.renderedElement.props),
    );
    // This render must wait for the Server Component even if a module
    // referenced by its output props has already failed to load or evaluate.
    const inputsReady =
      (metadata.renderEvent === undefined || metadata.renderEvent.released) &&
      canSettle(events);
    const errors =
      metadata.failure === undefined
        ? getReleasedErrors(events)
        : [fixture.valueMetadata.get(metadata.failure)];
    const models = findModels(expectedValue).concat(
      findModels(expectedValue.props),
      metadata.renderedElement === undefined
        ? []
        : findModels(metadata.renderedElement).concat(
            findModels(metadata.renderedElement.props),
          ),
    );
    return getResultExpectation(metadata.html, inputsReady, errors, models);
  }
  const check = registerResultCheck(
    state,
    expectedValue,
    path + '.render',
    getExpectation,
  );
  runAsyncTask(state, async () => {
    let html;
    try {
      html = await render(received);
    } catch (error) {
      handleRejection(state, check, error);
      return;
    }
    handleFulfillment(state, check, html);
  });
}

function observeStream(state, metadata, expectedValue, received, path) {
  const {fixture} = state;
  const items = metadata.items;
  const areItemsProducedThrough = i =>
    areAllEventsReleased(metadata.events.slice(0, i + 1));
  const isClosed = () => areAllEventsReleased(metadata.events);
  const getStreamErrors = () =>
    isClosed() && metadata.failure !== undefined
      ? [fixture.valueMetadata.get(metadata.failure)]
      : [];
  const canSettleThrough = i => {
    for (let j = 0; j <= i; j++) {
      if (!canSettle(getRequiredEvents(fixture, items[j]))) {
        return false;
      }
    }
    return true;
  };
  const getModelsThrough = (i, findModels) =>
    items.slice(0, i + 1).flatMap(findModels);
  function getItemExpectation(i, findModels) {
    const errors = getReleasedErrors(getRequiredEvents(fixture, items[i]));
    const expectation = getResultExpectation(
      items[i],
      areItemsProducedThrough(i) && canSettleThrough(i),
      errors,
      getModelsThrough(i, findModels),
    );
    // A stream error discards queued items. This read may receive an error
    // from a later item, even while an earlier item's inputs are pending.
    const laterErrors = items
      .slice(i + 1)
      .filter((item, offset) => areItemsProducedThrough(i + 1 + offset))
      .flatMap(item => getReleasedErrors(getRequiredEvents(fixture, item)))
      .concat(getStreamErrors());
    const referenceErrors = items
      .slice(i)
      .filter((item, offset) => areItemsProducedThrough(i + offset))
      .flatMap(item => getModelErrors(findModels(item)));
    if (laterErrors.length > 0 || referenceErrors.length > 0) {
      expectation.errors = laterErrors.concat(referenceErrors, errors);
    }
    return expectation;
  }
  function getEndExpectation(findModels) {
    const errors = getStreamErrors();
    return getResultExpectation(
      undefined,
      isClosed() && (errors.length > 0 || canSettleThrough(items.length - 1)),
      errors,
      getModelsThrough(items.length - 1, findModels),
    );
  }
  const itemChecks = items.map((item, i) =>
    registerResultCheck(state, item, path + '.item' + i, findModels =>
      getItemExpectation(i, findModels),
    ),
  );
  const endCheck = registerResultCheck(
    state,
    expectedValue,
    path + '.end',
    getEndExpectation,
  );
  runAsyncTask(state, async () => {
    const reader = received.getReader();
    for (let i = 0; i < items.length; i++) {
      let item;
      try {
        item = await reader.read();
      } catch (error) {
        handleRejection(state, itemChecks[i], error);
        // No further reads are possible after a stream error.
        for (let j = i + 1; j < items.length; j++) {
          itemChecks[j].done = true;
        }
        endCheck.done = true;
        return;
      }
      if (item.done) {
        throw new Error(path + '.item' + i + ': ended early');
      }
      handleFulfillment(state, itemChecks[i], item.value);
    }
    let last;
    try {
      last = await reader.read();
    } catch (error) {
      handleRejection(state, endCheck, error);
      return;
    }
    expect(last.done).toBe(true);
    handleFulfillment(state, endCheck, last.value);
  });
}

function observeAsyncIterable(state, metadata, received, path) {
  const {fixture} = state;
  // Use separate iterators for sequential and concurrent next() calls.
  // A single-use iterator can only run one of these checks.
  const modes = metadata.single ? [true] : [false, true];
  for (let mode = 0; mode < modes.length; mode++) {
    const readAhead = modes[mode];
    const iterator = received[Symbol.asyncIterator]();
    expect(iterator === received).toBe(metadata.single === true);
    const items = metadata.items;
    const consume = async i => {
      const done = i >= items.length;
      const value = done
        ? i === items.length
          ? metadata.returnValue
          : undefined
        : items[i];
      // Unlike a stream read, next() can settle before an earlier next().
      const check = registerResultCheck(
        state,
        value,
        path + (readAhead ? '.ahead' : '') + '.next' + i,
        findModels => {
          const events = getRequiredEvents(fixture, value);
          const inputsReady =
            areAllEventsReleased(metadata.events.slice(0, i + 1)) &&
            canSettle(events);
          return getResultExpectation(
            value,
            inputsReady,
            getReleasedErrors(events),
            findModels(value),
          );
        },
      );
      let result;
      try {
        result = await iterator.next();
      } catch (error) {
        // A rejected result doesn't prevent later next() calls.
        handleRejection(state, check, error);
        return;
      }
      expect(result.done).toBe(done);
      handleFulfillment(state, check, result.value);
    };
    // Reads after the return value must keep returning
    // {done: true, value: undefined}.
    if (readAhead) {
      for (let i = 0; i < items.length + 3; i++) {
        runAsyncTask(state, () => consume(i));
      }
    } else {
      runAsyncTask(state, async () => {
        for (let i = 0; i < items.length + 3; i++) {
          await consume(i);
        }
      });
    }
  }
}

function observeByteStream(state, metadata, expectedValue, received, path) {
  const expectedBytes = [].concat(...metadata.chunks.map(c => Array.from(c)));
  const getProducedByteCount = () => {
    let count = 0;
    for (let i = 0; i < metadata.chunks.length; i++) {
      if (!metadata.events[i].released) {
        break;
      }
      count += metadata.chunks[i].length;
    }
    return count;
  };
  runAsyncTask(state, async () => {
    const reader = received.getReader({mode: 'byob'});
    const receivedBytes = [];
    for (;;) {
      const offset = receivedBytes.length;
      // The default BYOB minimum is one byte. Don't wait for another
      // chunk to fill the buffer.
      const check = registerResultCheck(
        state,
        expectedValue,
        path + '.byte' + offset,
        () => {
          const available =
            getProducedByteCount() > offset ||
            areAllEventsReleased(metadata.events);
          return getResultExpectation(undefined, available, [], []);
        },
      );
      let result;
      try {
        result = await reader.read(
          new Uint8Array(1 + (receivedBytes.length % 3)),
        );
      } catch (error) {
        handleRejection(state, check, error);
        return;
      }
      const {done, value} = result;
      if (value) {
        receivedBytes.push(...value);
      }
      expect(receivedBytes).toEqual(
        expectedBytes.slice(
          0,
          Math.min(receivedBytes.length, getProducedByteCount()),
        ),
      );
      if (done) {
        expect(areAllEventsReleased(metadata.events)).toBe(true);
        expect(receivedBytes).toEqual(expectedBytes);
        handleFulfillment(state, check, undefined);
        break;
      }
      expect(receivedBytes.length).toBeGreaterThan(offset);
      handleFulfillment(state, check, undefined);
    }
  });
}

function observeCanceledStream(state, metadata, received, path) {
  const {fixture} = state;
  const check = registerResultCheck(
    state,
    metadata.first,
    path + '.first',
    findModels => {
      const events = getRequiredEvents(fixture, metadata.first);
      return getResultExpectation(
        metadata.first,
        canSettle(events),
        getReleasedErrors(events),
        findModels(metadata.first),
      );
    },
  );
  runAsyncTask(state, async () => {
    const reader = received.getReader();
    let item;
    try {
      item = await reader.read();
    } catch (error) {
      handleRejection(state, check, error);
      return;
    }
    expect(item.done).toBe(false);
    await reader.cancel('canceled by the test');
    handleFulfillment(state, check, item.value);
  });
}

function observeIterator(state, metadata, received) {
  // Cache the items so later checks don't consume the iterator again.
  let receivedItems = state.iteratorItems.get(received);
  if (receivedItems === undefined) {
    receivedItems = [];
    state.iteratorItems.set(received, receivedItems);
    for (let i = 0; i <= metadata.items.length; i++) {
      const step = received.next();
      expect(step.done).toBe(i === metadata.items.length);
      if (step.done) {
        expect(step.value).toBe(undefined);
      } else {
        receivedItems.push(step.value);
      }
    }
  }
}

function observeServerFunction(state, received, args) {
  state.functionCalls.set(received, {
    args,
    result: received(...args),
  });
}

function observeBlob(state, expectedValue, received) {
  runAsyncTask(state, () => assertBlobContents(expectedValue, received));
}

async function assertBlobContents(expectedValue, received) {
  expect(Array.from(new Uint8Array(await received.arrayBuffer()))).toEqual(
    Array.from(new Uint8Array(await expectedValue.arrayBuffer())),
  );
}

// Compare one value and return its children in traversal order.
// Return null for an object already checked in this traversal.
function compareValue(
  state,
  {expectedValue, received, path, isCallResult},
  visited,
) {
  const {fixture} = state;
  const children = [];
  if (
    expectedValue === null ||
    (typeof expectedValue !== 'object' && typeof expectedValue !== 'function')
  ) {
    expect(received).toBe(expectedValue);
    return [];
  }
  const metadata = fixture.valueMetadata.get(expectedValue);
  if (metadata !== undefined && metadata.type === 'module') {
    if (received !== metadata.value) {
      throw new Error(
        'expected the module export ' +
          describeValue(metadata.value) +
          ', received ' +
          describeValue(received),
      );
    }
    return [];
  }
  if (
    received === null ||
    (typeof received !== 'object' && typeof received !== 'function')
  ) {
    throw new Error(
      'expected ' +
        describeValue(expectedValue) +
        ', received ' +
        describeValue(received),
    );
  }
  if (!isCallResult) {
    // Several client objects may match one server object. A client object
    // must not match two different server objects.
    if (state.expectedByReceivedValue.has(received)) {
      if (state.expectedByReceivedValue.get(received) !== expectedValue) {
        throw new Error(
          'received ' +
            describeValue(received) +
            ' that was already delivered for a different value',
        );
      }
    } else {
      state.expectedByReceivedValue.set(received, expectedValue);
    }
    if (visited.has(received)) {
      return null;
    }
    visited.add(received);
  }
  if (metadata !== undefined) {
    switch (metadata.type) {
      case 'function':
        expect(typeof received).toBe('function');
        break;
      case 'promise':
        expect(typeof received.then).toBe('function');
        break;
      case 'error':
        expect(received instanceof Error).toBe(true);
        if (__DEV__) {
          expect(received.message).toBe(expectedValue.message);
          expect('cause' in received).toBe('cause' in expectedValue);
          if ('cause' in expectedValue) {
            children.push({
              expectedValue: expectedValue.cause,
              received: received.cause,
              path: path + '.cause',
            });
          }
          if (expectedValue instanceof AggregateError) {
            expect(received instanceof AggregateError).toBe(true);
            children.push({
              expectedValue: expectedValue.errors,
              received: received.errors,
              path: path + '.errors',
            });
          }
        }
        break;
      case 'iterator':
      case 'element':
      case 'stream':
      case 'iterable':
      case 'bytes':
      case 'canceled':
        break;
      default:
        throw new Error('Unknown value type: ' + metadata.type);
    }
  } else if (Array.isArray(expectedValue)) {
    if (!Array.isArray(received) || received.length !== expectedValue.length) {
      throw new Error(
        'expected ' +
          describeValue(expectedValue) +
          ', received ' +
          describeValue(received),
      );
    }
    for (let i = 0; i < expectedValue.length; i++) {
      children.push({
        expectedValue: expectedValue[i],
        received: received[i],
        path: path + '[' + i + ']',
      });
    }
  } else if (expectedValue instanceof Set) {
    if (!(received instanceof Set) || received.size !== expectedValue.size) {
      throw new Error(
        'expected ' +
          describeValue(expectedValue) +
          ', received ' +
          describeValue(received),
      );
    }
    const a = Array.from(expectedValue);
    const b = Array.from(received);
    for (let i = 0; i < a.length; i++) {
      children.push({
        expectedValue: a[i],
        received: b[i],
        path: path + '.member' + i,
      });
    }
  } else if (expectedValue instanceof Map) {
    if (!(received instanceof Map) || received.size !== expectedValue.size) {
      throw new Error(
        'expected ' +
          describeValue(expectedValue) +
          ', received ' +
          describeValue(received),
      );
    }
    const a = Array.from(expectedValue);
    const b = Array.from(received);
    for (let i = 0; i < a.length; i++) {
      children.push({
        expectedValue: a[i][0],
        received: b[i][0],
        path: path + '.key' + i,
      });
      children.push({
        expectedValue: a[i][1],
        received: b[i][1],
        path: path + '.value' + i,
      });
    }
  } else if (expectedValue instanceof Date) {
    if (
      !(received instanceof Date) ||
      received.getTime() !== expectedValue.getTime()
    ) {
      throw new Error(
        'expected ' +
          String(expectedValue) +
          ', received ' +
          describeValue(received),
      );
    }
  } else if (
    expectedValue instanceof ArrayBuffer ||
    ArrayBuffer.isView(expectedValue)
  ) {
    expect(Object.prototype.toString.call(received)).toBe(
      Object.prototype.toString.call(expectedValue),
    );
    // The writer may detach the source buffer. Use the bytes saved
    // before serialization.
    expect(getBytes(received)).toEqual(fixture.bytes.get(expectedValue));
  } else if (expectedValue instanceof Blob) {
    expect(received instanceof Blob).toBe(true);
    expect(received.type).toBe(expectedValue.type);
    expect(received.size).toBe(expectedValue.size);
  } else if (expectedValue instanceof FormData) {
    expect(received instanceof FormData).toBe(true);
    const a = Array.from(expectedValue.entries());
    const b = Array.from(received.entries());
    expect(b.length).toBe(a.length);
    for (let i = 0; i < a.length; i++) {
      expect(b[i][0]).toBe(a[i][0]);
      children.push({
        expectedValue: a[i][1],
        received: b[i][1],
        path: path + '.form' + i,
      });
    }
  } else {
    if (
      Object.getPrototypeOf(received) !== Object.prototype ||
      JSON.stringify(Object.keys(received)) !==
        JSON.stringify(Object.keys(expectedValue))
    ) {
      throw new Error(
        'expected ' +
          describeValue(expectedValue) +
          ', received ' +
          describeValue(received),
      );
    }
    const keys = Object.keys(expectedValue);
    for (let i = 0; i < keys.length; i++) {
      children.push({
        expectedValue: expectedValue[keys[i]],
        received: received[keys[i]],
        path: path + '.' + keys[i],
      });
    }
  }
  return children;
}

function getObservedChildren(state, {expectedValue, received, path}) {
  const metadata = state.fixture.valueMetadata.get(expectedValue);
  if (metadata === undefined) {
    return [];
  }
  switch (metadata.type) {
    case 'function': {
      const call = state.functionCalls.get(received);
      if (call === undefined) {
        throw new Error('A received Server Function was replaced');
      }
      // Compare bound arguments by identity; the result array is new per call.
      return [
        {
          expectedValue: metadata.result,
          received: call.result,
          path: path + '()',
          isCallResult: true,
        },
      ];
    }
    case 'iterator': {
      const items = state.iteratorItems.get(received);
      if (items === undefined) {
        throw new Error('A received iterator was replaced');
      }
      if (items.length !== metadata.items.length) {
        throw new Error(
          'expected an iterator of ' +
            metadata.items.length +
            ', received one of ' +
            items.length,
        );
      }
      return items.map((item, i) => ({
        expectedValue: metadata.items[i],
        received: item,
        path: path + '.item' + i,
      }));
    }
    case 'promise':
    case 'element':
    case 'stream':
    case 'iterable':
    case 'bytes':
    case 'canceled':
      if (!state.observedAsyncValues.has(received)) {
        throw new Error('A received async value was replaced');
      }
      return [];
    case 'module':
    case 'error':
      return [];
    default:
      throw new Error('Unknown value type: ' + metadata.type);
  }
}

async function recheckValues(state) {
  state.pendingTasks++;
  try {
    // These calls check that the functions still have their bound arguments.
    state.functionCalls.forEach((call, received) => {
      observeServerFunction(state, received, call.args);
    });
    const blobs = [];
    const count = state.receivedValues.length;
    for (let i = 0; i < count; i++) {
      const root = state.receivedValues[i];
      const todo = [
        {
          ...root,
          path: root.path + ' (after settlement)',
        },
      ];
      const visited = new Set();
      while (todo.length > 0) {
        const pair = todo.pop();
        try {
          const children = compareValue(state, pair, visited);
          if (children === null) {
            continue;
          }
          children.push(...getObservedChildren(state, pair));
          if (
            pair.expectedValue instanceof Blob &&
            !state.fixture.valueMetadata.has(pair.expectedValue)
          ) {
            blobs.push(pair);
          }
          for (let j = children.length - 1; j >= 0; j--) {
            todo.push(children[j]);
          }
        } catch (error) {
          throwValueError(error, pair.path);
        }
      }
    }
    await Promise.all(
      blobs.map(({expectedValue, received}) =>
        assertBlobContents(expectedValue, received),
      ),
    );
    throwIfFailed(state);
  } finally {
    state.pendingTasks--;
  }
}

function subscribe(state, thenable, check, subscribeAgain) {
  const settle = (value, rejected) => {
    guard(state, () => {
      // Repeated subscriptions must return the same value or error.
      const previous = state.thenableResults.get(thenable);
      if (previous === undefined) {
        state.thenableResults.set(thenable, {rejected, value});
      } else if (
        previous.rejected !== rejected ||
        !Object.is(previous.value, value)
      ) {
        throw new Error(
          'A thenable changed its settlement between observations',
        );
      }
    });
    if (rejected) {
      handleRejection(state, check, value);
    } else {
      handleFulfillment(state, check, value);
    }
    if (subscribeAgain) {
      const repeat = registerResultCheck(
        state,
        check.expectedValue,
        check.path + ' (again)',
        check.getExpectation,
      );
      subscribe(state, thenable, repeat, false);
    }
  };
  thenable.then(
    value => settle(value, false),
    error => settle(error, true),
  );
}

function describeEarlyResult(state, check) {
  const {fixture} = state;
  return (
    'Delivered before its inputs arrived: ' +
    check.path +
    '; not yet released: ' +
    getRequiredEvents(fixture, check.expectedValue)
      .filter(event => !event.released)
      .map(event => event.label)
      .join(', ')
  );
}

function throwIfFailed(state) {
  if (state.failures.length > 0) {
    throw state.failures[0];
  }
}

function recordFailure(state, error) {
  if (error.fuzzFailure === undefined) {
    error.fuzzFailure = 'observation: ' + error.message;
  }
  state.failures.push(error);
}

function guard(state, run) {
  try {
    run();
  } catch (error) {
    recordFailure(state, error);
  }
}

function runAsyncTask(state, run) {
  state.pendingTasks++;
  Promise.resolve()
    .then(run)
    .catch(error => recordFailure(state, error))
    .then(() => state.pendingTasks--);
}

function throwValueError(error, path) {
  if (error.fuzzFailure === undefined) {
    error.fuzzFailure = 'value: ' + error.message;
  }
  if (error.path === undefined) {
    error.message = path + ': ' + error.message;
    error.path = path;
  }
  throw error;
}

// Server errors are compared by digest; local module errors by identity.
function isExpectedError(error, expected) {
  return expected.some(candidate =>
    candidate instanceof Error
      ? candidate === error
      : error != null && candidate != null && error.digest === candidate.digest,
  );
}

function assertExpectedError(error, expected, path) {
  if (!isExpectedError(error, expected)) {
    throw new Error(
      path +
        ': rejected with "' +
        (error instanceof Error ? error.message : String(error)) +
        '", expected ' +
        expected
          .map(candidate =>
            candidate instanceof Error
              ? '"' + candidate.message + '"'
              : 'digest ' + candidate.digest,
          )
          .join(' or '),
    );
  }
}

function describeValue(value) {
  if (value === null || typeof value !== 'object') {
    return typeof value === 'function'
      ? 'a function'
      : typeof value === 'string'
        ? JSON.stringify(value)
        : String(value);
  }
  if (Array.isArray(value)) {
    return 'an array of ' + value.length;
  }
  if (value instanceof Set || value instanceof Map) {
    return 'a ' + value.constructor.name + ' of ' + value.size;
  }
  if (Object.getPrototypeOf(value) === Object.prototype) {
    return 'an object with keys ' + JSON.stringify(Object.keys(value));
  }
  return 'a ' + Object.prototype.toString.call(value).slice(8, -1);
}

function getBytes(view) {
  return view instanceof ArrayBuffer
    ? Array.from(new Uint8Array(view))
    : Array.from(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
}

// Only include children that can block decoding this value.
function getValueDependencies(fixture, value) {
  if (
    value === null ||
    (typeof value !== 'object' && typeof value !== 'function')
  ) {
    return [];
  }
  const metadata = fixture.valueMetadata.get(value);
  if (metadata !== undefined) {
    switch (metadata.type) {
      case 'function':
        return metadata.bound;
      case 'iterator':
        // A synchronous iterator must have all of its items available.
        return metadata.items;
      case 'error':
        return __DEV__
          ? [
              value.cause,
              value instanceof AggregateError ? value.errors : undefined,
            ]
          : [];
      default:
        return [];
    }
  }
  if (Array.isArray(value)) {
    return value;
  }
  if (value instanceof Set) {
    return Array.from(value);
  }
  if (value instanceof Map) {
    return Array.from(value.entries()).flat();
  }
  if (Object.getPrototypeOf(value) === Object.prototype) {
    return Object.values(value);
  }
  return [];
}

function getRequiredEvents(fixture, value) {
  const result = new Set();
  const seen = new Set();
  const todo = [value];
  while (todo.length) {
    const current = todo.pop();
    if (
      current !== null &&
      (typeof current === 'object' || typeof current === 'function')
    ) {
      if (seen.has(current)) {
        continue;
      }
      seen.add(current);
    }
    const metadata = fixture.valueMetadata.get(current);
    if (metadata !== undefined) {
      switch (metadata.type) {
        // These values can be returned before their contents are available.
        case 'promise':
        case 'element':
        case 'stream':
        case 'bytes':
        case 'canceled':
        case 'iterable':
          continue;
      }
    }
    (fixture.requiredEvents.get(current) || []).forEach(event =>
      result.add(event),
    );
    todo.push(...getValueDependencies(fixture, current));
  }
  return Array.from(result);
}

function areAllEventsReleased(events) {
  return Array.from(events).every(event => event.released);
}

function getReleasedErrors(events) {
  return Array.from(events)
    .filter(
      event =>
        event.released &&
        event.error !== undefined &&
        // The client can't see an evaluation error until the module loads.
        (event.after === undefined || event.after.released),
    )
    .map(event => event.error);
}

function canSettle(events) {
  return areAllEventsReleased(events) || getReleasedErrors(events).length > 0;
}

function collectModels(fixture) {
  const modelsByRootValue = new Map();
  // An object can occur in several models, e.g. two Promise results.
  const modelsByValue = new Map();
  const asyncValues = new Set();
  const boundArgumentModels = new Map();
  const visitedElements = new Set();
  function collectModel(rootValues, parent, events) {
    const model = {
      parent,
      events: new Set(events),
      referencedModels: new Set(),
    };
    const todo = rootValues.slice();
    const seen = new Set();
    while (todo.length) {
      const value = todo.pop();
      if (
        value === null ||
        (typeof value !== 'object' && typeof value !== 'function') ||
        seen.has(value)
      ) {
        continue;
      }
      seen.add(value);
      if (rootValues.includes(value)) {
        modelsByRootValue.set(value, model);
      }
      modelsByValue.set(value, (modelsByValue.get(value) || []).concat(model));
      const metadata = fixture.valueMetadata.get(value);
      if (metadata !== undefined) {
        switch (metadata.type) {
          case 'element':
            if (metadata.renderEvent !== undefined) {
              asyncValues.add(value);
            } else if (!visitedElements.has(value)) {
              // Errors decoding these props are reported when the element
              // renders. The containing model can still resolve.
              visitedElements.add(value);
              collectModel(
                [value.props],
                model,
                fixture.requiredEvents.get(value) || [],
              );
            }
            continue;
          case 'promise':
          case 'stream':
          case 'bytes':
          case 'canceled':
          case 'iterable':
            asyncValues.add(value);
            continue;
        }
      }
      (fixture.requiredEvents.get(value) || []).forEach(event =>
        model.events.add(event),
      );
      if (metadata !== undefined && metadata.type === 'function') {
        // Bound arguments are serialized separately, even in the root model.
        asyncValues.add(value);
        continue;
      }
      todo.push(...getValueDependencies(fixture, value));
    }
    return model;
  }
  const rootModel = collectModel(
    [fixture.resolvedModel !== null ? fixture.resolvedModel : fixture.model],
    null,
    [],
  );
  asyncValues.forEach(value => {
    const metadata = fixture.valueMetadata.get(value);
    switch (metadata.type) {
      case 'function':
        boundArgumentModels.set(value, collectModel(metadata.bound, null, []));
        break;
      case 'promise':
        if (metadata.state === 'fulfilled') {
          collectModel([metadata.value], null, []);
        }
        break;
      case 'element':
        collectModel([metadata.renderedElement], null, []);
        break;
      case 'stream':
        metadata.items.forEach(item => collectModel([item], null, []));
        break;
      case 'iterable':
        metadata.items.forEach(item => collectModel([item], null, []));
        collectModel([metadata.returnValue], null, []);
        break;
      case 'canceled':
        collectModel([metadata.first], null, []);
        break;
      default:
        break;
    }
  });
  boundArgumentModels.forEach((argumentModel, fn) => {
    modelsByValue
      .get(fn)
      .forEach(model => model.referencedModels.add(argumentModel));
  });
  const getTopLevelModel = model =>
    model.parent === null ? model : getTopLevelModel(model.parent);
  modelsByValue.forEach(models => {
    // The root is serialized first. Later models can reference its objects.
    const rootModels = models.filter(
      model => getTopLevelModel(model) === rootModel,
    );
    models.forEach(model => {
      // Otherwise, any of these models could serialize the object first.
      const possibleSources =
        rootModels.length > 0
          ? rootModels
          : models.filter(other => other !== model);
      possibleSources.forEach(sourceModel => {
        for (
          let referencedModel = sourceModel;
          referencedModel !== null;
          referencedModel = referencedModel.parent
        ) {
          model.referencedModels.add(referencedModel);
        }
      });
    });
  });
  return modelsByRootValue;
}

function createModelLookup(fixture) {
  let modelsByRootValue = null;
  return function getReferencedModels(value) {
    // Share the analysis within one synchronous check. Promise results may
    // add more source values before the next check.
    if (modelsByRootValue === null) {
      modelsByRootValue = collectModels(fixture);
    }
    const model = modelsByRootValue.get(value);
    if (model === undefined) {
      return [];
    }
    const result = [];
    const seen = new Set();
    const todo = [model];
    while (todo.length) {
      const current = todo.pop();
      if (!seen.has(current)) {
        seen.add(current);
        result.push(current);
        current.referencedModels.forEach(next => todo.push(next));
      }
    }
    return result;
  };
}

function getModelErrors(models) {
  return models.flatMap(model => getReleasedErrors(model.events));
}

module.exports = {createOracle};
