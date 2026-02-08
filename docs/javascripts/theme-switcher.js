// Theme switcher for LakeGuard documentation
// Enhances the Material for MkDocs theme toggle

document.addEventListener("DOMContentLoaded", function () {
  // Add smooth transitions when switching themes
  const observer = new MutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
      if (mutation.attributeName === "data-md-color-scheme") {
        document.body.style.transition =
          "background-color 0.3s ease, color 0.3s ease";
      }
    });
  });

  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["data-md-color-scheme"],
  });
});
