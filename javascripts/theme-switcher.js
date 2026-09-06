document.addEventListener("DOMContentLoaded", function () {
  // --- Feedback buttons ---
  document.querySelectorAll(".ll-feedback__btn").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var container = btn.closest(".ll-feedback__inner");
      if (!container) return;
      // Hide buttons, show thanks
      container.querySelectorAll(".ll-feedback__btn").forEach(function (b) {
        b.style.display = "none";
      });
      var thanks = container.querySelector(".ll-feedback__thanks");
      if (thanks) thanks.style.display = "inline";
      // Optional: send analytics event
      if (typeof gtag === "function") {
        gtag("event", "feedback", {
          event_category: "docs",
          event_label: btn.dataset.value,
          value: btn.dataset.value === "yes" ? 1 : 0,
        });
      }
    });
  });

  // --- Smooth scroll for anchor links ---
  document.querySelectorAll('a[href^="#"]').forEach(function (anchor) {
    anchor.addEventListener("click", function (e) {
      e.preventDefault();
      var target = document.querySelector(this.getAttribute("href"));
      if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  });

  // --- Copy success feedback ---
  document.querySelectorAll(".md-clipboard").forEach(function (button) {
    button.addEventListener("click", function () {
      var original = this.getAttribute("title");
      this.setAttribute("title", "Copied!");
      setTimeout(
        function () {
          button.setAttribute("title", original);
        },
        2000
      );
    });
  });

  // --- Intersection observer for admonitions/cards ---
  var observer = new IntersectionObserver(
    function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          entry.target.style.opacity = "1";
          entry.target.style.transform = "translateY(0)";
        }
      });
    },
    { threshold: 0.1, rootMargin: "0px 0px -50px 0px" }
  );

  document.querySelectorAll(".admonition, .card").forEach(function (el) {
    el.style.opacity = "0";
    el.style.transform = "translateY(20px)";
    el.style.transition = "all 0.5s ease";
    observer.observe(el);
  });
});
