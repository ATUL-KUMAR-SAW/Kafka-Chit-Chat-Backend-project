import './style.css'

const API_BASE = '/api/messages';

const chatContainer = document.getElementById('chatContainer');
const chatForm = document.getElementById('chatForm');
const messageInput = document.getElementById('messageInput');
const sendButton = document.getElementById('sendButton');
const inputActionIcons = document.getElementById('inputActionIcons');

// Setup Name Modal
const nameOverlay = document.getElementById('nameOverlay');
const nameForm = document.getElementById('nameForm');
const initialNameInput = document.getElementById('initialNameInput');

let currentUsername = localStorage.getItem('chatUsername') || '';

if (currentUsername) {
    nameOverlay.classList.add('hidden');
} else {
    // Focus the input when modal is visible
    setTimeout(() => initialNameInput.focus(), 100);
}

nameForm.onsubmit = (e) => {
    e.preventDefault();
    const name = initialNameInput.value.trim();
    if (name) {
        currentUsername = name;
        localStorage.setItem('chatUsername', currentUsername);
        nameOverlay.classList.add('hidden');
        messageInput.focus();
    }
};

let lastMessageCount = 0;

// Dynamic UI for Send button (Instagram style)
messageInput.addEventListener('input', () => {
    if (messageInput.value.trim().length > 0) {
        sendButton.classList.remove('hidden');
        inputActionIcons.classList.add('hidden');
    } else {
        sendButton.classList.add('hidden');
        inputActionIcons.classList.remove('hidden');
    }
});

// Redundant since the user typing updates the input itself seamlessly.
// We can omit the event listener entirely.

async function fetchMessages() {
  try {
    const response = await fetch(API_BASE);
    const messages = await response.json();
    
    if (messages.length > lastMessageCount) {
      renderMessages(messages);
      lastMessageCount = messages.length;
      scrollToBottom();
    }
  } catch (error) {
    console.error('Error fetching messages (is ApiServer running?):', error);
  }
}

function renderMessages(messages) {
  // Clear only messages, preserve the splash profile header
  Array.from(chatContainer.children).forEach(child => {
      if (!child.classList.contains('mb-10')) {
          child.remove();
      }
  });
  
  const currentUser = currentUsername || 'Anonymous';

  let lastSender = null;

  messages.forEach((msg, index) => {
    const isMe = msg.sender === currentUser;
    
    // Check if previous message was from same user for grouping
    const isConsecutive = lastSender === msg.sender;
    lastSender = msg.sender;
    
    const wrapperOuter = document.createElement('div');
    wrapperOuter.className = `flex flex-col max-w-xl ${isMe ? 'ml-auto items-end' : 'mr-auto items-start'} w-full group`;
    
    if (!isMe && !isConsecutive) {
        // Add sender name text block above bubble if not me and first in seq
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

    // Tooltip for timestamp (Instagram hover style)
    const timeTooltip = document.createElement('span');
    const date = new Date(msg.timestamp);
    const timeStr = `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;
    timeTooltip.textContent = timeStr;
    timeTooltip.className = `absolute top-1/2 -translate-y-1/2 text-xs text-igGrayText opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap ${isMe ? '-left-12' : '-right-12'}`;
    
    // Bubble
    const bubble = document.createElement('div');
    
    // Instagram tail logic
    let radiusClass = 'rounded-2xl px-[14px] py-[8px] text-[15px] leading-[18px]';
    if (isMe) {
        // Outgoing: White text, blue gradient background
        bubble.className = `${radiusClass} text-white bg-gradient-to-tr from-[#3797F0] to-[#E0245E] shadow-sm max-w-[85%]`;
        
        // Consecutive tailing tweaks
        if (isConsecutive) {
            bubble.classList.add('rounded-tr-md');
        } else {
            bubble.classList.add('rounded-br-2xl');
        }
        
    } else {
        // Incoming: White text, dark grey background
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

chatForm.onsubmit = async (e) => {
  e.preventDefault();
  
  const text = messageInput.value.trim();
  const sender = currentUsername || 'Anonymous';
  
  if (!text) return;

  // Prevent double submissions while sending
  if (sendButton.disabled) return;
  sendButton.disabled = true;
  
  messageInput.value = '';
  // Trigger input event manually to reset buttons
  messageInput.dispatchEvent(new Event('input'));

  const payload = {
    sender,
    text,
    timestamp: Date.now()
  };

  // Optimistic UI update
  // chatHistory array on backend handles source of truth
  
  try {
    await fetch(API_BASE, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });
    
    await fetchMessages();
  } catch (error) {
    console.error('Error sending message:', error);
  } finally {
    sendButton.disabled = false;
  }
};

setInterval(fetchMessages, 1000);
fetchMessages();
