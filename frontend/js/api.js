/* API helper — the frontend talks to the Master over the same origin. */
const API = (() => {
  async function request(method, path, body) {
    const opts = { method, headers: {} };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    const resp = await fetch(path, opts);
    const ct = resp.headers.get('content-type') || '';
    const data = ct.includes('application/json') ? await resp.json() : await resp.text();
    if (!resp.ok) {
      const msg = (data && typeof data === 'object' && data.error) ? data.error : ('HTTP ' + resp.status);
      throw new Error(msg);
    }
    return data;
  }
  return {
    get: (p) => request('GET', p),
    post: (p, b) => request('POST', p, b),
    put: (p, b) => request('PUT', p, b),
  };
})();
