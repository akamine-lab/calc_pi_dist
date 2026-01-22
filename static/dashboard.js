class Dashboard {
    constructor() {
        this.ws = null;
        this.reconnectDelay = 1000;
        this.maxReconnectDelay = 30000;
        this.results = [];
        this.maxResults = 50;
        this.initialRenderTimeout = null;

        this.elements = {
            statusIndicator: document.getElementById('statusIndicator'),
            connectionStatus: document.getElementById('connectionStatus'),
            queueLength: document.getElementById('queueLength'),
            inflightCount: document.getElementById('inflightCount'),
            resultsList: document.getElementById('resultsList'),
            resultsCount: document.getElementById('resultsCount'),
            piGrid: document.getElementById('piGrid'),
            piDecimal: document.getElementById('piDecimal')
        };

        // Pi digit state management
        this.digitStates = new Map(); // Map<digitIndex, {state: 'queue'|'inflight'|'result', value: string}>
        this.maxDigit = -1;
        this.correctPi = null; // Correct Pi digits from pi.txt

        this.loadCorrectPi();
        this.connect();
    }

    async loadCorrectPi() {
        try {
            const response = await fetch('/static/pi.txt');
            if (!response.ok) {
                console.error('Failed to load pi.txt:', response.status);
                return;
            }
            const text = await response.text();
            // Parse pi.txt: first line is "3.", rest are digits with spaces
            const lines = text.split('\n').filter(line => line.trim());
            if (lines.length === 0) {
                console.error('pi.txt is empty');
                return;
            }

            // First line should be "3."
            let piString = '3.';
            // Combine all other lines and remove spaces
            for (let i = 1; i < lines.length; i++) {
                piString += lines[i].replace(/\s/g, '');
            }

            this.correctPi = piString;
            console.log('Loaded correct Pi:', this.correctPi.substring(0, 50) + '...');

            // Re-render if we already have calculated digits
            // リロード時にも確実に10進数計算を実行
            if (this.maxDigit >= 0 || this.digitStates.size > 0) {
                this.renderPiDecimal();
            }
        } catch (error) {
            console.error('Error loading pi.txt:', error);
        }
    }

    connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}${window.location.pathname.replace(/\/static\/.*$/, '')}/ws`;

        try {
            this.ws = new WebSocket(wsUrl);

            this.ws.onopen = () => {
                console.log('WebSocket connected');
                this.updateConnectionStatus(true);
                this.reconnectDelay = 1000; // Reset delay on successful connection
                // Initial state will be sent by server
            };

            this.ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    this.handleMessage(data);
                } catch (e) {
                    console.error('Failed to parse WebSocket message:', e);
                }
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
                this.updateConnectionStatus(false);
            };

            this.ws.onclose = () => {
                console.log('WebSocket disconnected');
                this.updateConnectionStatus(false);
                this.scheduleReconnect();
            };
        } catch (e) {
            console.error('Failed to create WebSocket:', e);
            this.updateConnectionStatus(false);
            this.scheduleReconnect();
        }
    }

    scheduleReconnect() {
        setTimeout(() => {
            if (this.ws && this.ws.readyState === WebSocket.CLOSED) {
                console.log(`Attempting to reconnect (delay: ${this.reconnectDelay}ms)`);
                this.connect();
                this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
            }
        }, this.reconnectDelay);
    }

    updateConnectionStatus(connected) {
        if (connected) {
            this.elements.statusIndicator.classList.remove('disconnected');
            this.elements.statusIndicator.classList.add('connected');
            this.elements.connectionStatus.textContent = 'Connected';
        } else {
            this.elements.statusIndicator.classList.remove('connected');
            this.elements.statusIndicator.classList.add('disconnected');
            this.elements.connectionStatus.textContent = 'Disconnected';
        }
    }

    handleMessage(data) {
        switch (data.type) {
            case 'queue_update':
                this.updateQueueMetrics(data.queue_length, data.inflight_count);
                break;
            case 'job_update':
                this.updateJobStates(data.queue_jobs, data.inflight_jobs);
                // renderPiDigits()内でrenderPiDecimal()が呼ばれる
                break;
            case 'result':
                this.addResult(data);
                // addResult() → updateResultState() → renderPiDigits() → renderPiDecimal()が呼ばれる
                break;
            default:
                console.warn('Unknown message type:', data.type);
        }

        // リロード時：初期状態の処理後に10進数計算を確実に実行
        // resultメッセージ以外の場合のみ、タイムアウトで確実に実行
        // (resultメッセージの場合は既にrenderPiDecimal()が呼ばれている)
        if (data.type !== 'result' && this.ws && this.ws.readyState === WebSocket.OPEN) {
            clearTimeout(this.initialRenderTimeout);
            this.initialRenderTimeout = setTimeout(() => {
                if (this.maxDigit >= 0 || this.digitStates.size > 0) {
                    this.renderPiDecimal();
                }
            }, 100);
        }
    }

    updateQueueMetrics(queueLength, inflightCount) {
        this.elements.queueLength.textContent = queueLength;
        this.elements.inflightCount.textContent = inflightCount;
    }

    addResult(data) {
        // Add to beginning of array
        this.results.unshift({
            job_id: data.job_id,
            result: data.result,
            timestamp: data.timestamp || Math.floor(Date.now() / 1000)
        });

        // Keep only last maxResults
        if (this.results.length > this.maxResults) {
            this.results = this.results.slice(0, this.maxResults);
        }

        // Update pi digit states
        this.updateResultState(data.result);

        this.renderResults();
    }

    renderResults() {
        if (this.results.length === 0) {
            this.elements.resultsList.innerHTML = '<div class="empty-state">No results yet. Waiting for worker results...</div>';
            this.elements.resultsCount.textContent = '0';
            return;
        }

        this.elements.resultsCount.textContent = this.results.length;

        const html = this.results.map((result, index) => {
            const timestamp = this.formatTimestamp(result.timestamp);
            const resultJson = JSON.stringify(result.result, null, 2);
            const isNew = index === 0 && this.results.length > 0;

            return `
                <div class="result-item ${isNew ? 'new' : ''}">
                    <div class="result-header">
                        <span class="result-job-id">Job ID: ${result.job_id}</span>
                        <span class="result-timestamp">${timestamp}</span>
                    </div>
                    <div class="result-content">${this.escapeHtml(resultJson)}</div>
                </div>
            `;
        }).join('');

        this.elements.resultsList.innerHTML = html;
    }

    formatTimestamp(timestamp) {
        const date = new Date(timestamp * 1000);
        const now = new Date();
        const diff = Math.floor((now - date) / 1000);

        if (diff < 60) {
            return `${diff} seconds ago`;
        } else if (diff < 3600) {
            const minutes = Math.floor(diff / 60);
            return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
        } else if (diff < 86400) {
            const hours = Math.floor(diff / 3600);
            return `${hours} hour${hours > 1 ? 's' : ''} ago`;
        } else {
            return date.toLocaleString();
        }
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    hexFractionToDecimal(hexString, maxDigits = 1000) {
        // Convert hex fractional part to decimal using BigInt for high precision
        // hexString is the fractional part (e.g., "243F6A8885A3...")
        // Returns "3.141592653589793..." format

        if (!hexString || hexString.length === 0) {
            return '3.';
        }

        // Convert hex string to BigInt (treating it as an integer)
        // This represents the numerator of the fraction hexString / 16^hexString.length
        let numerator = BigInt(0);
        const hexDigits = hexString.toUpperCase();

        for (let i = 0; i < hexDigits.length; i++) {
            const hexDigit = hexDigits[i];
            const digitValue = BigInt(parseInt(hexDigit, 16));
            numerator = numerator * 16n + digitValue;
        }

        // Denominator is 16^hexString.length
        let denominator = 1n;
        for (let i = 0; i < hexDigits.length; i++) {
            denominator = denominator * 16n;
        }

        // Now convert the fraction numerator/denominator to decimal
        // We'll extract decimal digits one by one by:
        // 1. Multiply numerator by 10
        // 2. Divide by denominator to get next decimal digit
        // 3. Use remainder for next iteration

        let decimalResult = '3.';
        let currentNumerator = numerator;
        const digitsToShow = Math.min(hexDigits.length, maxDigits);

        for (let i = 0; i < digitsToShow; i++) {
            // Multiply by 10 to get next decimal digit
            currentNumerator = currentNumerator * 10n;

            // Divide by denominator to get the decimal digit
            const digit = currentNumerator / denominator;
            decimalResult += digit.toString();

            // Get remainder for next iteration
            currentNumerator = currentNumerator % denominator;

            // If remainder is 0, we've exhausted the precision
            if (currentNumerator === 0n) {
                break;
            }
        }

        return decimalResult;
    }

    getAllHexDigits() {
        // Collect all hex digits in order
        const hexArray = [];
        for (let i = 0; i <= this.maxDigit; i++) {
            const state = this.digitStates.get(i);
            if (state && state.state === 'result' && state.value) {
                hexArray[i] = state.value;
            }
        }

        // Build continuous hex string from index 0
        let hexString = '';
        for (let i = 0; i < hexArray.length; i++) {
            if (hexArray[i] !== undefined) {
                hexString += hexArray[i];
            } else {
                // Stop at first missing digit
                break;
            }
        }

        return hexString;
    }

    updateJobStates(queueJobs, inflightJobs) {
        // Track which digits are in queue or inflight
        const queueDigits = new Set();
        const inflightDigits = new Set();

        // Collect queue digit indices
        for (const job of queueJobs) {
            const payload = job.payload;
            if (payload && payload.type === 'bbp_hex' && payload.start !== undefined && payload.count !== undefined) {
                const start = parseInt(payload.start);
                const count = parseInt(payload.count);
                for (let i = 0; i < count; i++) {
                    const digitIndex = start + i;
                    queueDigits.add(digitIndex);
                    this.maxDigit = Math.max(this.maxDigit, digitIndex);
                }
            }
        }

        // Collect inflight digit indices
        for (const job of inflightJobs) {
            const payload = job.payload;
            if (payload && payload.type === 'bbp_hex' && payload.start !== undefined && payload.count !== undefined) {
                const start = parseInt(payload.start);
                const count = parseInt(payload.count);
                for (let i = 0; i < count; i++) {
                    const digitIndex = start + i;
                    inflightDigits.add(digitIndex);
                    this.maxDigit = Math.max(this.maxDigit, digitIndex);
                }
            }
        }

        // Update states: clear old queue/inflight states, but preserve result states
        for (const [digitIndex, state] of this.digitStates.entries()) {
            if (state.state === 'queue' || state.state === 'inflight') {
                // Remove if not in current queue or inflight
                if (!queueDigits.has(digitIndex) && !inflightDigits.has(digitIndex)) {
                    this.digitStates.delete(digitIndex);
                }
            }
        }

        // Set queue states (only if not result and not inflight)
        for (const digitIndex of queueDigits) {
            const currentState = this.digitStates.get(digitIndex);
            if (!currentState || currentState.state !== 'result') {
                // Only set queue if not already inflight
                if (!inflightDigits.has(digitIndex)) {
                    this.digitStates.set(digitIndex, { state: 'queue', value: null });
                }
            }
        }

        // Set inflight states (only if not result)
        for (const digitIndex of inflightDigits) {
            const currentState = this.digitStates.get(digitIndex);
            if (!currentState || currentState.state !== 'result') {
                this.digitStates.set(digitIndex, { state: 'inflight', value: null });
            }
        }

        this.renderPiDigits();
    }

    updateResultState(result) {
        if (result && result.hex && result.start !== undefined && result.count !== undefined) {
            const start = parseInt(result.start);
            const count = parseInt(result.count);
            const hex = result.hex;

            for (let i = 0; i < count && i < hex.length; i++) {
                const digitIndex = start + i;
                const value = hex[i];
                this.digitStates.set(digitIndex, { state: 'result', value: value });
                this.maxDigit = Math.max(this.maxDigit, digitIndex);
            }

            this.renderPiDigits();
        }
    }

    renderPiDigits() {
        if (this.maxDigit < 0) {
            this.elements.piGrid.innerHTML = '<div class="empty-state">Waiting for jobs...</div>';
            this.elements.piDecimal.innerHTML = '<div class="empty-state">Waiting for digits...</div>';
            return;
        }

        const digits = [];
        for (let i = 0; i <= this.maxDigit; i++) {
            const state = this.digitStates.get(i);
            let className = 'pi-digit';
            let hexValue = '?';

            if (state) {
                className += ` ${state.state}`;
                if (state.state === 'result' && state.value) {
                    hexValue = state.value;
                }
            } else {
                className += ' empty';
            }

            digits.push(`<span class="${className}">${this.escapeHtml(hexValue)}</span>`);
        }

        this.elements.piGrid.innerHTML = digits.join('');

        // Render decimal representation
        this.renderPiDecimal();
    }

    renderPiDecimal() {
        const hexString = this.getAllHexDigits();

        if (hexString.length === 0) {
            this.elements.piDecimal.innerHTML = '<div class="empty-state">Waiting for digits...</div>';
            return;
        }

        // 16進数n桁は約n * log10(16) ≈ n * 1.204桁の10進数に相当
        // 余裕を持たせて、16進数の桁数の1.5倍を計算する最大桁数とする
        // ただし、最小1000桁、最大10000桁まで
        const maxDecimalDigits = Math.min(Math.max(1000, Math.ceil(hexString.length * 1.5)), 10000);
        const decimalString = this.hexFractionToDecimal(hexString, maxDecimalDigits);

        // If we have correct Pi, highlight correct digits
        if (this.correctPi) {
            let html = '';
            for (let i = 0; i < decimalString.length; i++) {
                const char = decimalString[i];
                const isCorrect = i < this.correctPi.length && char === this.correctPi[i];

                if (isCorrect) {
                    html += `<span class="pi-correct-digit">${this.escapeHtml(char)}</span>`;
                } else {
                    html += this.escapeHtml(char);
                }
            }
            this.elements.piDecimal.innerHTML = html;
        } else {
            this.elements.piDecimal.textContent = decimalString;
        }
    }
}

// Initialize dashboard when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        new Dashboard();
    });
} else {
    new Dashboard();
}
