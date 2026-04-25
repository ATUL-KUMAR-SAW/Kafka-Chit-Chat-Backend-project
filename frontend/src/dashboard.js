const logContainer = document.getElementById('log-container');
const emptyState = document.getElementById('empty-state');
const filterErrorsCheckbox = document.getElementById('filter-errors');
const statusIndicator = document.getElementById('status-indicator');
const clearBtn = document.getElementById('clear-btn');

let ws;

function connectWebSocket() {
    const wsUrl = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1' 
        ? 'ws://localhost:8082/api/logs/stream'
        : `wss://${window.location.host}/api/logs/stream`;

    ws = new WebSocket(wsUrl);
    
    ws.onopen = () => {
        statusIndicator.innerHTML = `
            <span class="relative flex h-2.5 w-2.5">
                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span class="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500"></span>
            </span>
            <span class="text-emerald-400 tracking-widest text-[11px] font-bold">LIVE</span>
        `;
        statusIndicator.className = 'flex items-center justify-center min-w-[100px] gap-2 border px-4 py-1.5 rounded-full transition-colors bg-emerald-500/10 border-emerald-500/30 shadow-[0_0_10px_rgba(16,185,129,0.15)]';
    };
    
    ws.onmessage = (event) => {
        if (emptyState) emptyState.remove();

        try {
            const data = JSON.parse(event.data);
            appendLog(data);
        } catch (e) {
            console.error('Failed to parse log:', event.data);
        }
    };
    
    ws.onclose = () => {
        statusIndicator.innerHTML = `
            <span class="relative flex h-2.5 w-2.5">
                <span class="relative inline-flex rounded-full h-2.5 w-2.5 bg-red-500"></span>
            </span>
            <span class="text-red-500 tracking-widest text-[11px] font-bold">OFFLINE</span>
        `;
        statusIndicator.className = 'flex items-center justify-center min-w-[100px] gap-2 border px-4 py-1.5 rounded-full transition-colors bg-red-500/10 border-red-500/30 shadow-[0_0_10px_rgba(239,68,68,0.15)]';
        
        setTimeout(connectWebSocket, 3000);
    };
    
    ws.onerror = (err) => {
        console.error('WebSocket error:', err);
    };
}

function appendLog(log) {
    const isErrorFilter = filterErrorsCheckbox.checked;
    
    if (isErrorFilter && log.level !== 'SEVERE' && log.level !== 'WARNING') {
        return;
    }
    
    const div = document.createElement('div');
    // Datadog / Vercel style log row layout
    div.className = 'w-full px-8 py-2 border-b border-[#151515] hover:bg-[#0f0f0f] transition-colors flex items-start gap-4 font-mono text-[13px] animate-fade-in group cursor-default';
    
    // Time
    const timeDiv = document.createElement('div');
    timeDiv.className = 'w-32 flex-shrink-0 text-igGrayText pt-0.5 tracking-wider';
    const date = new Date(log.timestamp);
    timeDiv.textContent = `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}.${date.getMilliseconds().toString().padStart(3, '0')}`;
    
    // Component PIll 
    const compDiv = document.createElement('div');
    compDiv.className = 'w-24 flex-shrink-0 pt-0.5';
    const compPill = document.createElement('span');
    const isBroker = log.component.startsWith('Broker');
    compPill.className = `px-2.5 py-0.5 rounded text-[11px] font-bold tracking-wide ${isBroker ? 'bg-indigo-500/10 text-indigo-400 border border-indigo-500/20' : 'bg-purple-500/10 text-purple-400 border border-purple-500/20'}`;
    compPill.textContent = log.component;
    compDiv.appendChild(compPill);
    
    // Level Pill
    const levelDiv = document.createElement('div');
    levelDiv.className = 'w-24 flex-shrink-0 pt-0.5';
    const levelPill = document.createElement('span');
    levelPill.className = 'px-2.5 py-0.5 rounded text-[11px] font-bold tracking-wider border ' + getLevelClasses(log.level);
    levelPill.textContent = log.level;
    levelDiv.appendChild(levelPill);
    
    // Message Content
    const msgDiv = document.createElement('div');
    msgDiv.className = 'flex-1 break-words leading-relaxed pt-0.5 transition-colors ' + getMessageColor(log.level);
    msgDiv.textContent = log.message;
    
    div.append(timeDiv, compDiv, levelDiv, msgDiv);
    
    const isScrolledToBottom = logContainer.scrollHeight - logContainer.clientHeight <= logContainer.scrollTop + 50;
    
    logContainer.appendChild(div);
    
    if (logContainer.children.length > 500) {
        logContainer.removeChild(logContainer.firstChild); 
    }
    
    if (isScrolledToBottom) {
        logContainer.scrollTop = logContainer.scrollHeight;
    }
}

function getLevelClasses(level) {
    switch (level) {
        case 'SEVERE': return 'bg-red-500/10 text-red-500 border-red-500/30';
        case 'WARNING': return 'bg-amber-500/10 text-amber-500 border-amber-500/30';
        case 'INFO': return 'bg-blue-500/10 text-blue-400 border-blue-500/30';
        default: return 'bg-gray-500/10 text-gray-400 border-gray-500/30';
    }
}

function getMessageColor(level) {
    switch (level) {
        case 'SEVERE': return 'text-red-400 font-medium group-hover:text-red-300';
        case 'WARNING': return 'text-amber-300 group-hover:text-amber-200 font-medium';
        case 'INFO': return 'text-[#b3b3b3] group-hover:text-white';
        default: return 'text-gray-400 group-hover:text-gray-200';
    }
}

clearBtn.addEventListener('click', () => {
    logContainer.innerHTML = '';
});

filterErrorsCheckbox.addEventListener('change', () => {
    logContainer.innerHTML = '';
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close(); 
    }
});

connectWebSocket();
