import PQueue from '../source/index.js';
import delay from 'delay';

// 一格：concurrency 设成 1，在 active 回调里再 add 一个任务，看看同时在跑几个。
const run = async (variant: string) => {
	const queue = new PQueue({concurrency: 1});
	let inflight = 0;
	let maxInflight = 0;
	let peakPending = 0;
	const order: string[] = [];

	const observe = (label: string) => async () => {
		inflight++;
		maxInflight = Math.max(maxInflight, inflight);
		peakPending = Math.max(peakPending, queue.pending);
		order.push(label);
		await delay(30);
		inflight--;
	};

	let added = false;
	queue.on('active', () => {
		if (!added) {
			added = true;
			queue.add(observe('reentrant'));
		}
	});

	if (variant === 'solo') {
		await Promise.allSettled([queue.add(observe('A'))]);
	} else {
		await Promise.allSettled([queue.add(observe('A')), queue.add(observe('B')), queue.add(observe('C'))]);
	}

	await queue.onIdle();
	console.log(JSON.stringify({variant, maxInflight, peakPending, order: order.join('>')}));
};

for (const variant of ['solo', 'backlog']) {
	// eslint-disable-next-line no-await-in-loop
	await run(variant);
}

const control = new PQueue({concurrency: 1});
let inflight2 = 0;
let max2 = 0;
await Promise.allSettled([1, 2, 3].map(() => control.add(async () => {
	inflight2++;
	max2 = Math.max(max2, inflight2);
	await delay(30);
	inflight2--;
})));
console.log(`control(no listener) maxInflight=${max2}`);

// 另一格：readme 里那段 .on('add') 示例照抄，它写的期望是 Size: 0 / Pending: 1、Size: 0 / Pending: 2
const doc = new PQueue();
doc.on('add', () => {
	console.log(`Task is added.  Size: ${doc.size}  Pending: ${doc.pending}`);
});
doc.on('next', () => {
	console.log(`Task is completed.  Size: ${doc.size}  Pending: ${doc.pending}`);
});
const job1 = doc.add(() => delay(200));
const job2 = doc.add(() => delay(50));
await job1;
await job2;

// 第三格：四个生命周期事件各自报的 size / pending，和 readme 的 FAQ 那句「总数是 size + pending」
const trace: string[] = [];
const queue = new PQueue({concurrency: 1});
for (const event of ['add', 'active', 'completed', 'next', 'empty', 'idle']) {
	queue.on(event, () => trace.push(`${event} size=${queue.size} pending=${queue.pending} total=${queue.size + queue.pending}`));
}
const first = queue.add(() => delay(20));
trace.push(`awaited job1 -> total=${queue.size + queue.pending}`);
const second = queue.add(() => delay(20));
trace.push(`awaited job2 -> total=${queue.size + queue.pending}`);
await Promise.all([first, second]);
console.log(trace.join('\n'));
