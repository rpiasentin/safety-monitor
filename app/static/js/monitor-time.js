(function () {
  function parseTimestamp(value) {
    const text = String(value || '').trim();
    if (!text) return null;
    const stamp = new Date(text);
    return Number.isNaN(stamp.getTime()) ? null : stamp;
  }

  function formatTimestamp(value, mode) {
    const stamp = parseTimestamp(value);
    if (!stamp) return String(value || '');

    const optionsByMode = {
      full: {
        year: 'numeric',
        month: 'numeric',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        second: '2-digit',
        timeZoneName: 'short',
      },
      compact: {
        month: 'numeric',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
      },
      short: {
        hour: 'numeric',
        minute: '2-digit',
        second: '2-digit',
        timeZoneName: 'short',
      },
    };
    const options = optionsByMode[mode] || optionsByMode.full;
    return new Intl.DateTimeFormat(undefined, options).format(stamp);
  }

  function formatChartLabel(value) {
    return formatTimestamp(value, 'compact');
  }

  function hydrateTimestamps(root) {
    const scope = root || document;
    scope.querySelectorAll('[data-local-ts]').forEach((node) => {
      const raw = node.getAttribute('data-local-ts') || '';
      const mode = node.getAttribute('data-local-format') || 'full';
      const formatted = formatTimestamp(raw, mode);
      node.textContent = formatted;
      node.setAttribute('title', formatted);
    });
  }

  window.MonitorTime = {
    parseTimestamp,
    formatTimestamp,
    formatChartLabel,
    hydrateTimestamps,
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      hydrateTimestamps(document);
    });
  } else {
    hydrateTimestamps(document);
  }
})();
