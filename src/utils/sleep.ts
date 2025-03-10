export function sleep(ms: number = 1000) {
  console.log('Sleeping for', ms, 'ms');
  return new Promise((resolve) => setTimeout(resolve, ms));
}
