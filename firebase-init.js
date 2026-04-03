/*  firebase-init.js  â  AI Music Empire Firestore Integration
    Dynamically loads Firebase SDK and sets up realtime listeners.
    Drop <script src="firebase-init.js"></script> before </body>.        */

(function () {
  'use strict';

  /* ââ Firebase config ââââââââââââââââââââââââââââââââââââââââââââââ */
  var CFG = {
    apiKey: 'AIzaSyD0s3b77TswtXEdXCR8wBaya75WJMQQ71E',
    authDomain: 'ai-music-empire-d9ab3.firebaseapp.com',
    projectId: 'ai-music-empire-d9ab3',
    storageBucket: 'ai-music-empire-d9ab3.appspot.com',
    messagingSenderId: '123456789',
    appId: '1:123456789:web:abc123'
  };

  /* ââ Helpers ââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
  function $(id) { return document.getElementById(id); }

  function loadScript(url) {
    return new Promise(function (resolve, reject) {
      var s = document.createElement('script');
      s.src = url;
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    });
  }

  function fmt(n) {
    if (n == null || isNaN(n)) return 'â';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
  }

  function timeAgo(ts) {
    if (!ts) return '';
    var d = ts.toDate ? ts.toDate() : new Date(ts);
    var diff = (Date.now() - d.getTime()) / 1000;
    if (diff < 60) return 'just now';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    return Math.floor(diff / 86400) + 'd ago';
  }

  /* ââ Status indicator âââââââââââââââââââââââââââââââââââââââââââââ */
  function setStatus(ok) {
    var el = $('update-time');
    if (!el) return;
    if (ok) {
      el.textContent = 'Firestore Live Â· ' + new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
      el.style.color = '#4ade80';
    } else {
      el.textContent = 'Firestore Offline';
      el.style.color = '#f87171';
    }
  }

  /* ââ Channel map (Firestore doc id â DOM prefix) ââââââââââââââââââ */
  var CH_MAP = {
    'lofi_barista':   'ch1',
    'rain_walker':    'ch2',
    'velvet_groove':  'ch3',
    'piano_ghost':    'ch4'
  };

  /* ââ Listeners ââââââââââââââââââââââââââââââââââââââââââââââââââââ */
  function listen(db) {
    setStatus(true);

    /* --- channels collection ---- */
    db.collection('channels').onSnapshot(function (snap) {
      var totalSubs = 0, totalViews = 0, totalVids = 0;
      snap.forEach(function (doc) {
        var d = doc.data();
        var prefix = CH_MAP[doc.id];
        if (!prefix) return;

        var subs = d.subscribers || 0;
        var views = d.total_views || 0;
        var vids = d.video_count || 0;

        totalSubs += subs;
        totalViews += views;
        totalVids += vids;

        var elSubs = $(prefix + '-subs');
        var elViews = $(prefix + '-views');
        var elVids = $(prefix + '-videos');
        if (elSubs) elSubs.textContent = fmt(subs);
        if (elViews) elViews.textContent = fmt(views);
        if (elVids) elVids.textContent = fmt(vids);

        /* badge */
        var badge = $('badge-' + prefix);
        if (badge) {
          var status = d.status || 'Setting Up';
          badge.textContent = status;
          badge.style.background = status === 'Active' ? '#22c55e' : status === 'Error' ? '#ef4444' : '#f59e0b';
        }
      });

      var elTS = $('total-subs');
      var elTV = $('total-views');
      var elTVi = $('total-videos');
      if (elTS) elTS.textContent = fmt(totalSubs);
      if (elTV) elTV.textContent = fmt(totalViews);
      if (elTVi) elTVi.textContent = fmt(totalVids);

      setStatus(true);
    }, function () { setStatus(false); });

    /* --- revenue doc ---- */
    db.collection('revenue').doc('current').onSnapshot(function (doc) {
      if (!doc.exists) return;
      var d = doc.data();
      var el = $('est-revenue');
      if (el) el.textContent = (d.total_thb || 0).toLocaleString();

      /* progress bar */
      var target = 50000;
      var pct = Math.min(100, Math.round(((d.total_thb || 0) / target) * 100));
      var elPct = $('progress-pct');
      var elFill = $('progress-fill');
      if (elPct) elPct.textContent = pct + '%';
      if (elFill) elFill.style.width = pct + '%';
    });

    /* --- activity_log ---- */
    db.collection('activity_log')
      .orderBy('timestamp', 'desc')
      .limit(15)
      .onSnapshot(function (snap) {
        var list = $('log-list');
        if (!list) return;
        list.innerHTML = '';
        snap.forEach(function (doc) {
          var d = doc.data();
          var li = document.createElement('div');
          li.style.cssText = 'padding:6px 0;border-bottom:1px solid rgba(255,255,255,0.06);font-size:0.85rem;display:flex;justify-content:space-between;';
          var icon = d.type === 'error' ? 'ð´' : d.type === 'upload' ? 'ð¤' : d.type === 'milestone' ? 'ð' : 'ð¢';
          li.innerHTML = '<span>' + icon + ' ' + (d.message || '') + '</span><span style="color:#888;font-size:0.75rem;">' + timeAgo(d.timestamp) + '</span>';
          list.appendChild(li);
        });
        var lt = $('log-time');
        if (lt) lt.textContent = 'Updated ' + new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
      });

    /* --- uploads (latest video) ---- */
    db.collection('uploads')
      .orderBy('uploaded_at', 'desc')
      .limit(5)
      .onSnapshot(function (snap) {
        var el = $('latest-video');
        if (!el) return;
        if (snap.empty) { el.textContent = 'No videos uploaded yet'; return; }
        var html = '';
        snap.forEach(function (doc) {
          var d = doc.data();
          html += '<div style="padding:5px 0;border-bottom:1px solid rgba(255,255,255,0.06);font-size:0.85rem;">'
            + 'ð¹ ' + (d.title || 'Untitled').substring(0, 45)
            + '<span style="color:#888;font-size:0.75rem;float:right;">' + timeAgo(d.uploaded_at) + '</span></div>';
        });
        el.innerHTML = html;
      });

    /* --- pipeline_runs ---- */
    db.collection('pipeline_runs')
      .orderBy('started_at', 'desc')
      .limit(5)
      .onSnapshot(function (snap) {
        var total = 0, success = 0;
        var statusEl = $('pipeline-status');
        snap.forEach(function (doc) {
          var d = doc.data();
          total++;
          if (d.status === 'success') success++;
        });
        var pt = $('pipe-total');
        var ps = $('pipe-success');
        if (pt) pt.textContent = total;
        if (ps) ps.textContent = success;

        /* update pipeline steps based on latest run */
        if (!snap.empty) {
          var latest = snap.docs[0].data();
          var steps = latest.steps || {};
          var stepMap = {
            'step-generate': steps.generate || 'Pending',
            'step-video':    steps.video    || 'Pending',
            'step-thumb':    steps.thumbnail || 'Pending',
            'step-upload':   steps.upload   || 'Pending',
            'step-meta':     steps.metadata || 'Pending'
          };
          Object.keys(stepMap).forEach(function (id) {
            var el = $(id);
            if (!el) return;
            var val = stepMap[id];
            el.textContent = val;
            el.style.background = val === 'Done' ? '#22c55e' : val === 'Failed' ? '#ef4444' : '#f59e0b';
          });
          if (statusEl && latest.status) {
            var isOk = latest.status === 'success';
            statusEl.innerHTML = (isOk ? 'â' : 'â') + ' <strong>' + (isOk ? 'Success' : 'Failed') + '</strong> Â· Pipeline #' + (latest.run_number || '?');
          }
        }
      });
  }

  /* ââ Boot ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
  var SDK = 'https://www.gstatic.com/firebasejs/10.12.2/';
  loadScript(SDK + 'firebase-app-compat.js')
    .then(function () { return loadScript(SDK + 'firebase-firestore-compat.js'); })
    .then(function () {
      var app = firebase.initializeApp(CFG);
      var db  = firebase.firestore();
      listen(db);
      console.log('[AI Music Empire] Firestore connected');
    })
    .catch(function (err) {
      console.error('[AI Music Empire] Firebase load error', err);
      setStatus(false);
    });
})();
