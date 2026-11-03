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

const {
  generateGraph,
  generateScaleGraph,
  pruneGraph,
  shrinkGraph,
  formatGraphTest,
  formatReplayCommand,
} = require('./fuzz/FlightReaderFuzzGraph');

describe('FlightReaderFuzzGraph', () => {
  const graph = {
    nodes: {
      root: {array: ['cycle', 'noise']},
      cycle: {set: ['cycle', 'load']},
      load: {module: 'loaded'},
      noise: {value: 'not needed'},
    },
    roots: {entry: 'root', other: 'root'},
    events: ['load load'],
    packetSizes: [1, 3, 17],
    yieldBetweenPackets: true,
  };

  it('does not erase a Map branch with an equivalent primitive key', () => {
    const generated = generateGraph('queued-parser-stress:13');
    const zeros = Object.keys(generated.nodes).filter(name => {
      const node = generated.nodes[name];
      return node.value === 0 || node.special === 'negative zero';
    });
    expect(zeros.length).toBe(2);
    Object.values(generated.nodes).forEach(node => {
      if (node.map !== undefined) {
        const keys = node.map.filter(([key]) => zeros.includes(key));
        expect(keys.length).toBeLessThanOrEqual(1);
      }
    });
  });

  it('samples depth, width, and cycle size reproducibly', () => {
    const seen = new Map();
    for (let i = 0; i < 64; i++) {
      const seed = i === 0 ? 'default' : 'default:' + i;
      const sampled = generateScaleGraph(seed);
      expect(sampled).toEqual(generateScaleGraph(seed));
      const {shape, size, graph: generatedGraph} = sampled;
      seen.set(shape, Math.max(seen.get(shape) || 0, size));
      expect(pruneGraph(generatedGraph)).toEqual(generatedGraph);
      if (shape === 'bound') {
        let name = generatedGraph.roots.entry;
        let depth = 0;
        while (generatedGraph.nodes[name].bound !== undefined) {
          name = generatedGraph.nodes[name].bound[0];
          depth++;
        }
        expect(depth).toBe(size);
      }
    }
    expect(seen.get('bound')).toBe(2048);
    expect(seen.get('wide')).toBe(256);
    expect(seen.get('cycle')).toBe(256);
  });

  it('reduces a failure without changing its schedule or its original graph', async () => {
    const original = JSON.stringify(graph);
    const attempts = [];
    const failure = {error: 'cycle with a loading member'};
    const reproduces = async candidate => {
      const cycle = candidate.nodes.cycle;
      const reproduced =
        cycle !== undefined &&
        Array.isArray(cycle.set) &&
        cycle.set.includes('cycle') &&
        cycle.set.includes('load') &&
        candidate.nodes.load.module === 'loaded';
      attempts.push({key: JSON.stringify(candidate), reproduced});
      return reproduced ? failure : false;
    };
    const result = await shrinkGraph(graph, reproduces, 100);
    expect(Object.keys(result.graph.nodes)).toEqual(['cycle', 'load']);
    expect(result.graph.events).toEqual(graph.events);
    expect(result.graph.packetSizes).toEqual(graph.packetSizes);
    expect(result.graph.yieldBetweenPackets).toBe(true);
    expect(result.reproduction).toBe(failure);
    expect(result.attempts).toBe(attempts.length);
    expect(JSON.stringify(graph)).toBe(original);
    // Successful candidates are always replayed again before acceptance.
    for (let i = 0; i < attempts.length; i++) {
      if (attempts[i].reproduced) {
        expect(attempts[++i]).toEqual(attempts[i - 1]);
      }
    }
  });

  it('does not accept a flaky candidate or exceed the replay budget', async () => {
    let calls = 0;
    const result = await shrinkGraph(graph, async () => ++calls % 2 === 1, 2);
    expect(calls).toBe(2);
    expect(result.attempts).toBe(2);
    expect(result.graph).toEqual(graph);
    expect(result.reproduction).toBe(null);
    const withUnreachableNode = {
      ...graph,
      nodes: {...graph.nodes, unreachable: {value: 'leave it alone'}},
    };
    const noBudget = await shrinkGraph(
      withUnreachableNode,
      () => {
        throw new Error('not enough budget to confirm a candidate');
      },
      1,
    );
    expect(noBudget.attempts).toBe(0);
    expect(noBudget.graph).toBe(withUnreachableNode);
  });

  it('prints a graph literal without corrupting quotes, escapes, or names', () => {
    const name = 'a "node" with an apostrophe\' and a slash\\';
    const original = {
      nodes: {[name]: {value: 'quotes: "\'\\\n🙂 and looksLikeKey: value'}},
      roots: {entry: name, other: name},
      events: ['event "\'\\\n'],
      packetSizes: [1, 19],
      yieldBetweenPackets: false,
      rootPromise: true,
    };
    let printed;
    require('vm').runInNewContext(formatGraphTest(original, 'after-eof'), {
      it(title, run) {
        run();
      },
      runGraph(value) {
        printed = value;
      },
    });
    expect(printed).toEqual({...original, readTiming: 'after-eof'});
  });

  it('replays the exact host, read timing, schedule, seed, and production mode', () => {
    const seed = "a'b $(literal)";
    const name =
      'ReactFlightReaderFuzz (node) generated graphs graph seed=' +
      seed +
      ' readTiming=after-eof schedule=1';
    const command = formatReplayCommand(seed, name, false);
    expect(command).toContain("FUZZ_TEST_SEED='a'\\''b $(literal)'");
    expect(command).toContain('FLIGHT_FUZZ_CASES=1');
    expect(command).toContain('ReactFlightReaderFuzz --prod --runInBand');
    const quoted = command.slice(command.lastIndexOf(' -t ') + 4);
    const pattern = quoted.slice(1, -1).replace(/'\\''/g, "'");
    const expression = new RegExp(pattern);
    expect(expression.test(name)).toBe(true);
    expect(expression.test(name.replace('(node)', '(edge)'))).toBe(false);
    expect(expression.test(name.replace('after-eof', 'early'))).toBe(false);
    expect(expression.test(name.replace('schedule=1', 'schedule=0'))).toBe(
      false,
    );
    expect(expression.test(name + ' more')).toBe(false);
    expect(formatReplayCommand(seed, name, true)).not.toContain('--prod');
  });
});
