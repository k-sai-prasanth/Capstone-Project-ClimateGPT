async function sendMessage() {
    const input = document.getElementById('user-input');
    const question = input.value.trim();
    if (!question) return; // Ignore empty messages
    input.value = '';

    // Create a user message in the chat window
    const responseBox = document.createElement('div');
    responseBox.className = 'user-message';
    responseBox.textContent = question;
    document.getElementById('chat-window').appendChild(responseBox);

    // Scroll to the bottom after adding new message
    document.getElementById('chat-window').scrollTop = document.getElementById('chat-window').scrollHeight;

    // Fetch the answer from the server
    const response = await fetch('/ask', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ question })
    });

    const data = await response.json();

    // Create a bot response in the chat window
    const botResponseBox = document.createElement('div');
    botResponseBox.className = 'bot-response';
    botResponseBox.textContent = data.response;
    document.getElementById('chat-window').appendChild(botResponseBox);

    // Scroll to the bottom after adding new response
    document.getElementById('chat-window').scrollTop = document.getElementById('chat-window').scrollHeight;
}
