export function normalizeUrl(url: string): string {
    try {
        const parsedObj = new URL(url);
        
        // Remove tracking parameters for Instagram and TikTok
        if (parsedObj.hostname.includes('instagram.com') || parsedObj.hostname.includes('tiktok.com')) {
            parsedObj.search = ''; // Strips everything after the '?'
        }
        
        // For YouTube, keep the '?v=...' but remove 'feature=share', etc.
        if (parsedObj.hostname.includes('youtube.com')) {
            const videoId = parsedObj.searchParams.get('v');
            if (videoId) {
                return `https://www.youtube.com/watch?v=${videoId}`;
            }
        }
        
        // Handle short youtu.be links
        if (parsedObj.hostname.includes('youtu.be')) {
            return `https://www.youtube.com/watch?v=${parsedObj.pathname.slice(1)}`;
        }

        return parsedObj.toString().replace(/\/$/, ""); // Remove trailing slash
    } catch (e) {
        return url; // Fallback
    }
}