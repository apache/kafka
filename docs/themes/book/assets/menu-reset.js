(function() {
  var menu = document.querySelector("aside.book-menu nav");
  addEventListener("beforeunload", function(event) {
      localStorage.setItem("menu.scrollTop", menu.scrollTop);
  });
  menu.scrollTop = localStorage.getItem("menu.scrollTop");
})();
