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

const {createFixture} = require('./fuzz/FlightReaderFuzzFixture');

describe('FlightReaderFuzzFixture', () => {
  it.each([
    [{value: 0}, {special: 'negative zero'}],
    [{special: 'nan'}, {special: 'nan'}],
    [{special: 'symbol'}, {special: 'symbol'}],
    [{value: null}, {value: null}],
  ])('rejects a source Map whose keys %j and %j collide', async (a, b) => {
    const graph = {
      nodes: {
        a,
        b,
        earlier: {value: 'erased'},
        later: {value: 'kept'},
        map: {
          map: [
            ['a', 'earlier'],
            ['b', 'later'],
          ],
        },
      },
      roots: {entry: 'map', other: 'map'},
    };
    await expect(createFixture(graph, {})).rejects.toThrow(
      'map has two entries for the same source Map key',
    );
  });
});
