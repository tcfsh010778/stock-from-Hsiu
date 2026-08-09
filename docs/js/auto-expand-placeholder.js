document.addEventListener('DOMContentLoaded', async () => {
    const primaryNav = document.querySelector('nav');
    if (primaryNav && !primaryNav.querySelector('a.tab[href$="holder-risers.html"]')) {
        const flowLink = primaryNav.querySelector('a.tab[href$="institutional-flow.html"]');
        const holderLink = document.createElement('a');
        const flowHref = flowLink ? flowLink.getAttribute('href') : 'institutional-flow.html';
        holderLink.href = flowHref.replace(/institutional-flow\.html$/, 'holder-risers.html');
        holderLink.className = 'tab';
        holderLink.textContent = '大戶股權';
        holderLink.dataset.holderNav = 'true';
        if (flowLink) {
            flowLink.insertAdjacentElement('afterend', holderLink);
        } else {
            primaryNav.appendChild(holderLink);
        }
    }

    const blocks = document.querySelectorAll('.placeholder-block[data-source]');
    for (const block of blocks) {
        const src = block.dataset.source;
        try {
            const res = await fetch(src, { method: 'HEAD', cache: 'no-store' });
            if (res.ok) {
                block.open = true;
                block.classList.add('data-ready');
            }
        } catch (e) {
            // Keep collapsed when the future data file is not published yet.
        }
    }
});
