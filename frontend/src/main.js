import './style.css'

const API_BASE = '/api/messages';

// ── DOM Elements ──
const chatContainer = document.getElementById('chatContainer');
const chatForm = document.getElementById('chatForm');
const messageInput = document.getElementById('messageInput');
const sendButton = document.getElementById('sendButton');
const inputActionIcons = document.getElementById('inputActionIcons');

// ── Name Modal Elements ──
const nameOverlay = document.getElementById('nameOverlay');
const nameForm = document.getElementById('nameForm');
const initialNameInput = document.getElementById('initialNameInput');

// ── State ──
let currentUsername = localStorage.getItem('chatUsername') || '';
let lastMessageCount = 0;
let isSending = false;  // Global lock to prevent any double submissions

// ── Name Modal Logic ──
if (currentUsername) {
    // User already joined before — hide overlay immediately
    nameOverlay.style.display = 'none';
} else {
    nameOverlay.style.display = 'flex';
    setTimeout(() => initialNameInput.focus(), 200);
}

nameForm.onsubmit = (e) => {
    e.preventDefault();
    e.stopPropagation();
    const name = initialNameInput.value.trim();
    if (!name) return;
    
    currentUsername = name;
    localStorage.setItem('chatUsername', currentUsername);
    nameOverlay.style.display = 'none';
    messageInput.focus();
};

// ── Send Button Show/Hide (Instagram style) ──
messageInput.oninput = () => {
    if (messageInput.value.trim().length > 0) {
        sendButton.classList.remove('hidden');
        inputActionIcons.classList.add('hidden');
    } else {
        sendButton.classList.add('hidden');
        inputActionIcons.classList.remove('hidden');
    }
};

// ── Fetch Messages ──
async function fetchMessages() {
    try {
        const response = await fetch(API_BASE);
        if (!response.ok) return;
        const messages = await response.json();

        if (messages.length !== lastMessageCount) {
            renderMessages(messages);
            lastMessageCount = messages.length;
            scrollToBottom();
        }
    } catch (error) {
        // Silently ignore — backend may not be running yet
    }
}

// ── Render Messages ──
function renderMessages(messages) {
    // Clear only dynamic messages, preserve the splash profile header
    Array.from(chatContainer.children).forEach(child => {
        if (!child.classList.contains('mb-10')) {
            child.remove();
        }
    });

    const currentUser = currentUsername || 'Anonymous';
    let lastSender = null;

    messages.forEach((msg) => {
        const isMe = msg.sender === currentUser;
        const isConsecutive = lastSender === msg.sender;
        lastSender = msg.sender;

        const wrapperOuter = document.createElement('div');
        wrapperOuter.className = `flex flex-col max-w-xl ${isMe ? 'ml-auto items-end' : 'mr-auto items-start'} w-full group`;

        if (!isMe && !isConsecutive) {
            wrapperOuter.classList.add('mt-[18px]');
            const nameEl = document.createElement('span');
            nameEl.textContent = msg.sender;
            nameEl.className = 'text-[11px] text-igGrayText font-semibold ml-2 mb-[2px]';
            wrapperOuter.appendChild(nameEl);
        } else if (!isConsecutive) {
            wrapperOuter.classList.add('mt-[12px]');
        } else {
            wrapperOuter.classList.add('mt-[2px]');
        }

        const wrapperInner = document.createElement('div');
        wrapperInner.className = 'flex relative w-full items-center ' + (isMe ? 'justify-end' : 'justify-start');

        // Timestamp tooltip (hover)
        const timeTooltip = document.createElement('span');
        const date = new Date(msg.timestamp);
        const timeStr = `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;
        timeTooltip.textContent = timeStr;
        timeTooltip.className = `absolute top-1/2 -translate-y-1/2 text-xs text-igGrayText opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap ${isMe ? '-left-12' : '-right-12'}`;

        // Bubble
        const bubble = document.createElement('div');
        let radiusClass = 'rounded-2xl px-[14px] py-[8px] text-[15px] leading-[18px]';

        if (isMe) {
            bubble.className = `${radiusClass} text-white bg-gradient-to-tr from-[#3797F0] to-[#E0245E] shadow-sm max-w-[85%]`;
            if (isConsecutive) {
                bubble.classList.add('rounded-tr-md');
            } else {
                bubble.classList.add('rounded-br-2xl');
            }
        } else {
            bubble.className = `${radiusClass} text-igPrimaryText bg-[#262626] border border-[#363636] max-w-[85%]`;
            if (isConsecutive) {
                bubble.classList.add('rounded-tl-md');
            }
        }

        const textP = document.createElement('p');
        textP.className = 'break-words';
        textP.textContent = msg.text;
        bubble.appendChild(textP);

        wrapperInner.appendChild(bubble);
        wrapperInner.appendChild(timeTooltip);
        wrapperOuter.appendChild(wrapperInner);
        chatContainer.appendChild(wrapperOuter);
    });
}

function scrollToBottom() {
    setTimeout(() => {
        chatContainer.scrollTop = chatContainer.scrollHeight;
    }, 50);
}

// ── Send Message ──
chatForm.onsubmit = async (e) => {
    e.preventDefault();
    e.stopPropagation();

    const text = messageInput.value.trim();
    const sender = currentUsername || 'Anonymous';

    if (!text) return;

    // Hard lock — prevents any double/triple submission
    if (isSending) return;
    isSending = true;

    // Immediately clear the input
    messageInput.value = '';
    sendButton.classList.add('hidden');
    inputActionIcons.classList.remove('hidden');

    const payload = {
        sender,
        text,
        timestamp: Date.now()
    };

    try {
        const response = await fetch(API_BASE, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (response.ok) {
            await fetchMessages();
        }
    } catch (error) {
        console.error('Error sending message:', error);
    } finally {
        isSending = false;
    }
};

// ── Polling ──
setInterval(fetchMessages, 1500);
fetchMessages();
