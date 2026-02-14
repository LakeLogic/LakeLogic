// Custom JavaScript for theme enhancements

document.addEventListener("DOMContentLoaded", function () {
  // Add smooth scroll behavior for anchor links
  document.querySelectorAll('a[href^="#"]').forEach((anchor) => {
    anchor.addEventListener("click", function (e) {
      e.preventDefault();
      const target = document.querySelector(this.getAttribute("href"));
      if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  });

  // Add copy success feedback for code blocks
  const copyButtons = document.querySelectorAll(".md-clipboard");
  copyButtons.forEach((button) => {
    button.addEventListener("click", function () {
      const originalText = this.getAttribute("title");
      this.setAttribute("title", "✅ Copied!");
      setTimeout(() => {
        this.setAttribute("title", originalText);
      }, 2000);
    });
  });

  // Add animation class to cards on scroll
  const observerOptions = {
    threshold: 0.1,
    rootMargin: "0px 0px -50px 0px",
  };

  const observer = new IntersectionObserver(function (entries) {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        entry.target.style.opacity = "1";
        entry.target.style.transform = "translateY(0)";
      }
    });
  }, observerOptions);

  // Observe all admonitions and cards
  document.querySelectorAll(".admonition, .card").forEach((el) => {
    el.style.opacity = "0";
    el.style.transform = "translateY(20px)";
    el.style.transition = "all 0.5s ease";
    observer.observe(el);
  });
});
