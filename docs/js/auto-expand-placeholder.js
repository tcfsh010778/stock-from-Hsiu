document.addEventListener('DOMContentLoaded', async () => {
    const blocks = document.querySelectorAll('.placeholder-block[data-source]');
    for (const block of blocks) {
        const src = block.dataset.source;
        try {
            const res = await fetch(src, { method: 'HEAD', cache: 'no-store' });
            if (res.ok) {
                block.open = true;
                block.classList.add('data-ready', 'ready');
            }
        } catch (e) {
            // Keep collapsed when the future data file is not published yet.
        }
    }
});
