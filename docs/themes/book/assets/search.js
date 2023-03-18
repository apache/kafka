'use strict';

{{ $searchDataFile := printf "%s.search-data.js" .Language.Lang }}
{{ $searchData := resources.Get "search-data.js" | resources.ExecuteAsTemplate $searchDataFile . | resources.Minify | resources.Fingerprint }}

(function () {
  const input = document.querySelector('#book-search-input');
  const results = document.querySelector('#book-search-results');

  if (!input) {
    return
  }

  input.addEventListener('focus', init);
  input.addEventListener('keyup', search);

  document.addEventListener('keypress', focusSearchFieldOnKeyPress);

  /**
   * @param {Event} event
   */
  function focusSearchFieldOnKeyPress(event) {
    if (input === document.activeElement) {
      return;
    }

    const characterPressed = String.fromCharCode(event.charCode);
    if (!isHotkey(characterPressed)) {
      return;
    }

    input.focus();
    event.preventDefault();
  }

  /**
   * @param {String} character
   * @returns {Boolean} 
   */
  function isHotkey(character) {
    const dataHotkeys = input.getAttribute('data-hotkeys') || '';
    return dataHotkeys.indexOf(character) >= 0;
  }

  function init() {
    input.removeEventListener('focus', init); // init once
    input.required = true;

    loadScript('{{ "flexsearch.min.js" | relURL }}');
    loadScript('{{ $searchData.RelPermalink }}', function () {
      input.required = false;
      search();
    });
  }

  function search() {
    while (results.firstChild) {
      results.removeChild(results.firstChild);
    }

    if (!input.value) {
      return;
    }

    const searchHits = window.bookSearchIndex.search(input.value, 10);
    searchHits.forEach(function (page) {
      const li = element('<li><a href></a><small></small></li>');
      const a = li.querySelector('a'), small = li.querySelector('small');

      a.href = page.href;
      a.textContent = page.title;
      small.textContent = page.section;

      results.appendChild(li);
    });
  }

  /**
   * @param {String} src 
   * @param {Function} callback 
   */
  function loadScript(src, callback) {
    const script = document.createElement('script');
    script.defer = true;
    script.async = false;
    script.src = src;
    script.onload = callback;

    document.head.appendChild(script);
  }

  /**
   * @param {String} content
   * @returns {Node}
   */
  function element(content) {
    const div = document.createElement('div');
    div.innerHTML = content;
    return div.firstChild;
  }
})();
