(function () {
  var pref = 'system';
  try {
    var v = window.localStorage.getItem('fraudwatch.theme');
    if (v === 'light' || v === 'dark' || v === 'system') pref = v;
  } catch (e) {}
  var dark = pref === 'dark' || (pref === 'system' && window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches);
  document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
  document.documentElement.setAttribute('data-theme-preference', pref);
})();
