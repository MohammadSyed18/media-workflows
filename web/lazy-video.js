/** Attach only the source needed for a visible trailer; detach on cleanup. */
export function observeTrailers(root=document) {
 const videos=[...root.querySelectorAll('video[data-src]')];
 const load=video=>{if(video.dataset.src){video.src=video.dataset.src;delete video.dataset.src;video.load()}};
 if(!('IntersectionObserver' in globalThis)){videos.forEach(load);return()=>{}};
 const observer=new IntersectionObserver(entries=>{for(const entry of entries)if(entry.isIntersecting){load(entry.target);observer.unobserve(entry.target)}},{rootMargin:'200px'});
 videos.forEach(video=>observer.observe(video));
 return()=>observer.disconnect();
}
