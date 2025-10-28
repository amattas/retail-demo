// Retail Data Generator Web Interface JavaScript

class RetailDataGenerator {
    constructor() {
        this.baseUrl = '';
        this.currentTab = 'dashboard';
        this.streamingInterval = null;
        this.masterTableNames = [
            'geographies_master',
            'stores',
            'distribution_centers',
            'trucks',
            'customers',
            'products_master'
        ];
        this.factTableNames = [
            'dc_inventory_txn',
            'truck_moves',
            'store_inventory_txn',
            'receipts',
            'receipt_lines',
            'foot_traffic',
            'ble_pings',
            'marketing',
            'online_orders'
        ];
        this._lastCountRefresh = {};
        this._tableCountVersions = {};
        this._countVersionSeq = 0;
        this.init();
    }

    async init() {
        this.setupEventListeners();

        // Determine initial tab before any loading to avoid flicker
        let initialTab = 'dashboard';
        try {
            const saved = localStorage.getItem('activeTab');
            const validTabs = new Set(['dashboard','master-data','historical','streaming','config']);
            if (saved && validTabs.has(saved)) initialTab = saved;
        } catch (_) { /* ignore */ }

        // Activate the initial tab immediately
        await this.switchTab(initialTab);

        // Then perform initialization tasks
        await this.checkSystemHealth();
        await this.loadConfiguration();
        await this.loadGenerationState();
        await this.updateTableCounts();
        if (initialTab === 'dashboard') {
            await this.updateDashboardStats();
        } else if (initialTab === 'master-data') {
            await Promise.all(this.masterTableNames.map(name => this.ensureTableCount(name)));
            await this.updateAllTablesData();
        } else if (initialTab === 'historical') {
            // already updated counts above
        } else if (initialTab === 'streaming') {
            await this.updateStreamingStatus();
        }
        this.setDefaultDates();

        // Check for active tasks and reconnect if needed
        await this.checkForActiveTasks();
    }

    setupEventListeners() {
        // Tab navigation
        document.querySelectorAll('.nav-tab').forEach(tab => {
            tab.addEventListener('click', (e) => {
                const tabName = e.currentTarget.dataset.tab;
                void this.switchTab(tabName);
            });
        });

        // Close modal when clicking outside
        window.addEventListener('click', (e) => {
            const modal = document.getElementById('previewModal');
            if (e.target === modal) {
                this.closePreview();
            }
        });

        // Clickable cards: event delegation so clicks on any child trigger preview
        const masterGrid = document.querySelector('#master-data .table-grid');
        if (masterGrid) {
            masterGrid.addEventListener('click', (e) => {
                const item = e.target.closest('.table-item');
                if (!item || !masterGrid.contains(item)) return;
                const table = item.dataset.table;
                if (table) this.previewTable(table, 'Master Data');
            });
        }
        const histGrid = document.querySelector('#historical .table-grid');
        if (histGrid) {
            histGrid.addEventListener('click', (e) => {
                const item = e.target.closest('.table-item');
                if (!item || !histGrid.contains(item)) return;
                const table = item.dataset.table;
                if (table) this.previewTable(table, 'Historical Data');
            });
        }
    }

    async checkSystemHealth() {
        try {
            const response = await fetch('/health');
            const health = await response.json();
            
            const statusEl = document.getElementById('statusText');
            statusEl.classList.remove('online','offline');
            if (health.status === 'healthy') {
                statusEl.classList.add('online');
                statusEl.innerHTML = '<span class="status-dot" aria-hidden="true"></span><span>System Online</span>';
            } else {
                statusEl.classList.add('offline');
                statusEl.innerHTML = '<span class="status-x" aria-hidden="true">×</span><span>System Issues</span>';
            }
        } catch (error) {
            console.error('Health check failed:', error);
            const statusEl = document.getElementById('statusText');
            statusEl.classList.remove('online');
            statusEl.classList.add('offline');
            statusEl.innerHTML = '<span class="status-x" aria-hidden="true">×</span><span>Connection Failed</span>';
        }
    }

    async loadGenerationState() {
        try {
            const response = await fetch('/api/generation/status');
            const state = await response.json();
            
            // Update UI to show generation state information
            this.displayGenerationState(state);
            
        } catch (error) {
            console.error('Failed to load generation state:', error);
        }
    }

    async displayGenerationState(state) {
        // Find or create a status display area in the historical tab
        const historicalTab = document.getElementById('historical');
        let statusDiv = historicalTab.querySelector('.generation-status');

        if (!statusDiv) {
            statusDiv = document.createElement('div');
            statusDiv.className = 'generation-status card';
            statusDiv.innerHTML = `
                <h3>Generation Status</h3>
                <div class="status-info"></div>
            `;
            // Insert after the section header
            const sectionHeader = historicalTab.querySelector('.section-header');
            sectionHeader.insertAdjacentElement('afterend', statusDiv);
        }

        const statusInfo = statusDiv.querySelector('.status-info');

        // Check if we have fact data by querying the cache
        let hasFactData = state.has_historical_data;
        if (!hasFactData) {
            try {
                const cacheResponse = await fetch('/api/dashboard/counts');
                if (cacheResponse.ok) {
                    const cachedData = await cacheResponse.json();
                    hasFactData = cachedData.fact_tables && Object.keys(cachedData.fact_tables).length > 0;
                }
            } catch (err) {
                // Fallback to checking fact tables directly
                try {
                    const factTablesResponse = await fetch('/api/facts/tables');
                    if (factTablesResponse.ok) {
                        const factTablesData = await factTablesResponse.json();
                        hasFactData = factTablesData.tables && factTablesData.tables.length > 0;
                    }
                } catch (err2) {
                    console.log('Could not determine fact data status');
                }
            }
        }

        if (hasFactData) {
            statusInfo.innerHTML = `
                <p><strong>Historical Data:</strong> ✅ Generated</p>
                ${state.last_generated_timestamp ? `<p><strong>Last Generated:</strong> ${new Date(state.last_generated_timestamp).toLocaleString()}</p>` : ''}
                <p><strong>Real-time Ready:</strong> ${state.can_start_realtime ? '✅ Yes' : '❌ No'}</p>
                ${state.last_historical_run ? `<p><strong>Last Run:</strong> ${new Date(state.last_historical_run).toLocaleString()}</p>` : ''}
            `;
        } else {
            statusInfo.innerHTML = `
                <p><strong>Historical Data:</strong> ❌ Not generated yet</p>
                <p><strong>Status:</strong> Run historical generation first</p>
                <p><strong>Real-time Ready:</strong> ❌ No</p>
            `;
        }
    }

    async loadConfiguration() {
        try {
            const response = await fetch('/api/config');
            const config = await response.json();
            
            // Update configuration form fields
            if (config.volume) {
                document.getElementById('stores').value = config.volume.stores || 250;
                document.getElementById('dcs').value = config.volume.dcs || 12;
                document.getElementById('customersPerDay').value = config.volume.customers_per_day || 20000;
                document.getElementById('itemsPerTicket').value = config.volume.items_per_ticket_mean || 4.2;
                if (document.getElementById('onlineOrdersPerDay')) {
                    document.getElementById('onlineOrdersPerDay').value = config.volume.online_orders_per_day || 2500;
                }
                if (document.getElementById('marketingImpressionsPerDay')) {
                    document.getElementById('marketingImpressionsPerDay').value = config.volume.marketing_impressions_per_day || 10000;
                }
            }
            
            if (config.realtime) {
                document.getElementById('emitInterval').value = config.realtime.emit_interval_ms || 500;
                document.getElementById('burstSize').value = config.realtime.burst || 100;
                
                // Handle Azure connection string (sensitive data)
                const connectionStringField = document.getElementById('connectionString');
                if (config.realtime.azure_connection_string) {
                    connectionStringField.placeholder = 'Connection string configured (hidden for security)';
                } else {
                    connectionStringField.placeholder = 'Enter Azure Event Hub connection string';
                }
            }
            
            if (config.stream) {
                document.getElementById('hubName').value = config.stream.hub || 'retail-events';
            }

        } catch (error) {
            console.error('Failed to load configuration:', error);
            this.showNotification('Failed to load configuration', 'error');
        }
    }

    async updateDashboardStats() {
        try {
            // Initialize all counts to 0 with visual styling
            const statElements = ['storeCount', 'customerCount', 'productCount', 'transactionCount'];
            statElements.forEach(id => {
                const element = document.getElementById(id);
                element.textContent = '0';
                element.classList.add('no-data');
            });

            let storeCount = null;
            let customerCount = null;
            let productCount = null;
            let receiptCount = null;

            // Try to get cached counts first
            try {
                const cacheResponse = await fetch('/api/dashboard/counts');
                if (cacheResponse.ok) {
                    const cachedData = await cacheResponse.json();
                    if (cachedData.master_tables) {
                        storeCount = this.resolveCountValue(cachedData.master_tables['stores']);
                        customerCount = this.resolveCountValue(cachedData.master_tables['customers']);
                        productCount = this.resolveCountValue(cachedData.master_tables['products_master']);
                    }
                    if (cachedData.fact_tables) {
                        receiptCount = this.resolveCountValue(cachedData.fact_tables['receipts']);
                    }
                }
            } catch (err) {
                console.log('Dashboard cache not available, will backfill counts directly');
            }

            // Backfill any missing master counts directly
            const masterFallbacks = [
                { key: 'stores', elementId: 'storeCount' },
                { key: 'customers', elementId: 'customerCount' },
                { key: 'products_master', elementId: 'productCount' },
            ];

            for (const { key, elementId } of masterFallbacks) {
                const currentValue =
                    key === 'stores' ? storeCount :
                    key === 'customers' ? customerCount :
                    key === 'products_master' ? productCount :
                    null;

                if (currentValue !== null) {
                    continue;
                }

                try {
                    const response = await fetch(`/api/master/${key}`);
                    if (response.ok) {
                        const result = await response.json();
                        const count = typeof result.row_count === 'number' ? result.row_count : null;
                        if (count !== null) {
                            switch (key) {
                                case 'stores':
                                    storeCount = count;
                                    break;
                                case 'customers':
                                    customerCount = count;
                                    break;
                                case 'products_master':
                                    productCount = count;
                                    break;
                                default:
                                    break;
                            }

                            const element = document.getElementById(elementId);
                            element.textContent = count.toLocaleString();
                            element.classList.remove('no-data');
                        }
                    }
                } catch (err) {
                    console.warn(`Failed to backfill ${key} count:`, err);
                }
            }

            // Backfill receipts if needed
            if (receiptCount === null) {
                try {
                    const response = await fetch('/api/facts/receipts');
                    if (response.ok) {
                        const result = await response.json();
                        if (typeof result.total_records === 'number') {
                            receiptCount = result.total_records;
                        }
                    }
                } catch (err) {
                    console.warn('Failed to backfill receipts count:', err);
                }
            }

            // Apply counts to stat cards
            if (typeof storeCount === 'number') {
                const element = document.getElementById('storeCount');
                element.textContent = storeCount.toLocaleString();
                element.classList.remove('no-data');
            }
            if (typeof customerCount === 'number') {
                const element = document.getElementById('customerCount');
                element.textContent = customerCount.toLocaleString();
                element.classList.remove('no-data');
            }
            if (typeof productCount === 'number') {
                const element = document.getElementById('productCount');
                element.textContent = productCount.toLocaleString();
                element.classList.remove('no-data');
            }
            if (typeof receiptCount === 'number') {
                const transactionElement = document.getElementById('transactionCount');
                transactionElement.textContent = receiptCount.toLocaleString();
                transactionElement.classList.remove('no-data');
            }

            // Update full tables summary once counts have been refreshed
            await this.updateAllTablesData();

        } catch (error) {
            console.error('Failed to update dashboard stats:', error);
        }
    }

    updateTableStatus(tableName, status) {
        const item = document.querySelector(`.table-item[data-table="${tableName}"]`);
        if (!item) return;

        item.classList.remove(
            'table-status-processing',
            'table-status-completed',
            'table-status-failed'
        );

        if (status === 'processing') {
            item.classList.add('table-status-processing');
        const currentEl = document.getElementById(`count-${tableName}`);
        const previous = currentEl ? Number(currentEl.dataset.countValue) : NaN;
        if (!Number.isNaN(previous) && previous > 0) {
            this.setTableCount(tableName, previous);
        } else {
            this.setTableCount(tableName, null, 'Generating...');
        }
        } else if (status === 'completed') {
            item.classList.add('table-status-completed');
            this.refreshTableCount(tableName);
        } else if (status === 'failed') {
            item.classList.add('table-status-failed');
            this.setTableCount(tableName, null, 'Failed');
        } else {
            this.setTableCount(tableName, null);
        }
    }

    clearTableStatuses(tables) {
        tables.forEach(table => {
            this.updateTableStatus(table, null);
            delete this._tableCountVersions[table];
            this.setTableCount(
                table,
                null,
                '0 records',
                { updateVersionOnFallback: true }
            );
        });
    }

    clearHistoricalTableStatuses() {
        this.clearTableStatuses([
            'dc_inventory_txn',
            'truck_moves',
            'store_inventory_txn',
            'receipts',
            'receipt_lines',
            'foot_traffic',
            'ble_pings',
            'marketing',
        ]);
    }

    clearMasterTableStatuses() {
        this.clearTableStatuses(this.masterTableNames);
    }

    _nextCountVersion() {
        this._countVersionSeq = (this._countVersionSeq || 0) + 1;
        return this._countVersionSeq;
    }

    resolveCountValue(entry) {
        if (typeof entry === 'number') {
            return entry;
        }
        if (typeof entry === 'string') {
            const parsed = Number(entry);
            return Number.isNaN(parsed) ? null : parsed;
        }
        if (entry && typeof entry.count === 'number') {
            return entry.count;
        }
        if (entry && typeof entry.count === 'string') {
            const parsed = Number(entry.count);
            return Number.isNaN(parsed) ? null : parsed;
        }
        return null;
    }

    setTableCount(tableName, value, fallbackText = null, options = {}) {
        const {
            version = this._nextCountVersion(),
            allowOverwrite = false,
            updateVersionOnFallback = false,
        } = options;

        const countEl = document.getElementById(`count-${tableName}`);
        if (!countEl) return;
        const item = document.querySelector(`.table-item[data-table="${tableName}"]`);
        const lastVersion = this._tableCountVersions[tableName] ?? -Infinity;

        if (!allowOverwrite && version < lastVersion) {
            return;
        }

        if (typeof value === 'number') {
            countEl.dataset.countValue = String(value);
            countEl.textContent = `${value.toLocaleString()} records`;
            countEl.classList.remove('no-data');
            this._tableCountVersions[tableName] = version;
            return;
        }

        if (fallbackText) {
            if (item && item.classList.contains('table-status-processing')) {
                return;
            }
            countEl.textContent = fallbackText;
            if (fallbackText === '0 records') {
                delete countEl.dataset.countValue;
                countEl.classList.add('no-data');
            } else {
                countEl.classList.remove('no-data');
            }
            if (updateVersionOnFallback) {
                this._tableCountVersions[tableName] = version;
            }
            return;
        }

        if (countEl.dataset.countValue) {
            const cached = Number(countEl.dataset.countValue);
            if (!Number.isNaN(cached)) {
                countEl.textContent = `${cached.toLocaleString()} records`;
                countEl.classList.remove('no-data');
                if (updateVersionOnFallback) {
                    this._tableCountVersions[tableName] = version;
                }
                return;
            }
        }

        countEl.textContent = '0 records';
        delete countEl.dataset.countValue;
        countEl.classList.add('no-data');
        if (updateVersionOnFallback) {
            this._tableCountVersions[tableName] = version;
        }
    }

    async ensureTableCount(tableName) {
        const countEl = document.getElementById(`count-${tableName}`);
        const current = countEl ? Number(countEl.dataset.countValue) : NaN;
        if (Number.isNaN(current) || current <= 0) {
            await this.refreshTableCount(tableName);
        }
    }

    async refreshTableCount(tableName) {
        try {
            const requestVersion = this._nextCountVersion();
            if (this.masterTableNames.includes(tableName)) {
                const response = await fetch(`/api/master/${tableName}?limit=1`);
                if (!response.ok) {
                    if (response.status === 404) {
                        this.setTableCount(
                            tableName,
                            null,
                            '0 records',
                            { version: requestVersion, updateVersionOnFallback: true }
                        );
                    }
                    return;
                }
                const result = await response.json();
                const count = typeof result.row_count === 'number' ? result.row_count : null;
                if (count !== null) {
                    this.setTableCount(tableName, count, null, { version: requestVersion });
                }
                return;
            }

            if (this.factTableNames.includes(tableName)) {
                const response = await fetch(`/api/facts/${tableName}`);
                if (!response.ok) {
                    if (response.status === 404) {
                        this.setTableCount(
                            tableName,
                            null,
                            '0 records',
                            { version: requestVersion, updateVersionOnFallback: true }
                        );
                    }
                    return;
                }
                const result = await response.json();
                const count = typeof result.total_records === 'number' ? result.total_records : null;
                if (count !== null) {
                    this.setTableCount(tableName, count, null, { version: requestVersion });
                }
            }
        } catch (error) {
            console.warn(`Failed to refresh count for ${tableName}:`, error);
        }
    }

    maybeRefreshTableCount(tableName) {
        const now = Date.now();
        const last = this._lastCountRefresh[tableName] || 0;
        if (now - last < 1000) {
            return;
        }
        const item = document.querySelector(`.table-item[data-table="${tableName}"]`);
        if (item && item.classList.contains('table-status-processing')) {
            return;
        }
        this._lastCountRefresh[tableName] = now;
        this.refreshTableCount(tableName);
    }

    async updateAllTablesData() {
        try {
            const tableContainer = document.getElementById('allTablesData');
            if (!tableContainer) return;

            const allTables = [];

            // Master data tables
            const masterTables = [
                { name: 'geographies_master', displayName: 'Geographies', icon: 'fas fa-map-marker-alt', type: 'Master Data' },
                { name: 'stores', displayName: 'Stores', icon: 'fas fa-store', type: 'Master Data' },
                { name: 'distribution_centers', displayName: 'Distribution Centers', icon: 'fas fa-warehouse', type: 'Master Data' },
                { name: 'trucks', displayName: 'Trucks', icon: 'fas fa-truck', type: 'Master Data' },
                { name: 'customers', displayName: 'Customers', icon: 'fas fa-users', type: 'Master Data' },
                { name: 'products_master', displayName: 'Products', icon: 'fas fa-box', type: 'Master Data' }
            ];

            // Fact tables
            const factTables = [
                { name: 'receipts', displayName: 'Receipts', icon: 'fas fa-receipt', type: 'Historical Data' },
                { name: 'receipt_lines', displayName: 'Receipt Lines', icon: 'fas fa-list', type: 'Historical Data' },
                { name: 'store_inventory_txn', displayName: 'Store Inventory', icon: 'fas fa-boxes', type: 'Historical Data' },
                { name: 'dc_inventory_txn', displayName: 'DC Inventory', icon: 'fas fa-warehouse', type: 'Historical Data' },
                { name: 'truck_moves', displayName: 'Truck Moves', icon: 'fas fa-truck-moving', type: 'Historical Data' },
                { name: 'foot_traffic', displayName: 'Foot Traffic', icon: 'fas fa-walking', type: 'Historical Data' },
                { name: 'ble_pings', displayName: 'BLE Pings', icon: 'fas fa-wifi', type: 'Historical Data' },
                { name: 'marketing', displayName: 'Marketing', icon: 'fas fa-bullhorn', type: 'Historical Data' },
                { name: 'online_orders', displayName: 'Online Orders', icon: 'fas fa-shopping-bag', type: 'Historical Data' }
            ];

            // Try to use cached data first (fast path)
            try {
                const cacheResponse = await fetch('/api/dashboard/counts');
                if (cacheResponse.ok) {
                    const cachedData = await cacheResponse.json();

                    // Process master tables from cache
                    for (const table of masterTables) {
                        const count = this.resolveCountValue(cachedData.master_tables?.[table.name]);
                        allTables.push({
                            ...table,
                            count: count !== null ? count : 0,
                            status: count !== null ? 'Generated' : 'Not Generated'
                        });
                    }

                    // Backfill missing master table counts directly if cache empty
                    const missingMasterTables = allTables.filter(
                        table => table.type === 'Master Data' && table.status !== 'Generated'
                    );
                    for (const table of missingMasterTables) {
                        try {
                            const response = await fetch(`/api/master/${table.name}`);
                            if (response.ok) {
                                const result = await response.json();
                                table.count = result.row_count || 0;
                                table.status = result.row_count ? 'Generated' : 'Not Generated';
                            }
                        } catch (err) {
                            console.warn(`Failed to backfill count for ${table.name}:`, err);
                        }
                    }

                    // Process fact tables from cache, with on-demand summary fallback for counts
                    for (const table of factTables) {
                        let count = this.resolveCountValue(cachedData.fact_tables?.[table.name]);
                        let status = count !== null ? 'Generated' : 'Not Generated';
                        // Fallback: query summary if cache missing
                        if (count === null) {
                            try {
                                const summaryResp = await fetch(`/api/facts/${table.name}`);
                                if (summaryResp.ok) {
                                    const summary = await summaryResp.json();
                                    count = summary.total_records || 0;
                                    status = count > 0 ? 'Generated' : 'Not Generated';
                                }
                            } catch (e) {
                                // Ignore, leave as 0
                            }
                        }
                        allTables.push({ ...table, count: count || 0, status });
                    }
                } else {
                    throw new Error('Cache not available');
                }
            } catch (err) {
                console.log('Using direct queries for table data');

                // Fallback to direct queries if cache fails
                for (const table of masterTables) {
                    try {
                        const response = await fetch(`/api/master/${table.name}`);
                        if (response.ok) {
                            const result = await response.json();
                            allTables.push({
                                ...table,
                                count: result.row_count || 0,
                                status: 'Generated'
                            });
                        } else {
                            allTables.push({
                                ...table,
                                count: 0,
                                status: 'Not Generated'
                            });
                        }
                    } catch (err) {
                        allTables.push({
                            ...table,
                            count: 0,
                            status: 'Not Generated'
                        });
                    }
                }

                // Query fact tables
                try {
                    const factTablesResponse = await fetch('/api/facts/tables');
                    if (factTablesResponse.ok) {
                        const factTablesData = await factTablesResponse.json();
                        const existingFactTables = factTablesData.tables || [];

                        for (const table of factTables) {
                            if (existingFactTables.includes(table.name)) {
                                // Fetch summary to get actual count
                                try {
                                    const summaryResp = await fetch(`/api/facts/${table.name}`);
                                    if (summaryResp.ok) {
                                        const summary = await summaryResp.json();
                                        allTables.push({ ...table, count: summary.total_records || 0, status: (summary.total_records || 0) > 0 ? 'Generated' : 'Not Generated' });
                                    } else {
                                        allTables.push({ ...table, count: 0, status: 'Generated' });
                                    }
                                } catch (e) {
                                    allTables.push({ ...table, count: 0, status: 'Generated' });
                                }
                            } else {
                                allTables.push({ ...table, count: 0, status: 'Not Generated' });
                            }
                        }
                    }
                } catch (err) {
                    console.log('No fact tables available yet');
                    for (const table of factTables) {
                        allTables.push({
                            ...table,
                            count: 0,
                            status: 'Not Generated'
                        });
                    }
                }
            }

            // Generate table HTML
            const tableHTML = `
                <div class="tables-summary">
                    <h3>All Tables & Records</h3>
                    <div class="table-container">
                        <table class="data-tables-summary">
                            <thead>
                                <tr>
                                    <th>Table</th>
                                    <th>Type</th>
                                    <th>Records</th>
                                    <th>Status</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${allTables.map(table => `
                                    <tr class="${table.status === 'Generated' ? 'table-generated' : 'table-not-generated'}">
                                        <td>
                                            <i class="${table.icon}"></i>
                                            ${table.displayName}
                                        </td>
                                        <td>${table.type}</td>
                                        <td class="record-count">
                                            ${typeof table.count === 'number' ? table.count.toLocaleString() : table.count}
                                        </td>
                                        <td>
                                            <span class="status-badge ${table.status === 'Generated' ? 'status-success' : 'status-pending'}">
                                                ${table.status}
                                            </span>
                                        </td>
                                        <td>
                                            ${table.status === 'Generated' ?
                                                `<i class="fas fa-eye action-icon" onclick="app.previewTable('${table.name}', '${table.type}')" title="Preview ${table.displayName}"></i>` :
                                                '<span class="text-muted">—</span>'
                                            }
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;

            tableContainer.innerHTML = tableHTML;

        } catch (error) {
            console.error('Failed to update all tables data:', error);
        }
    }

    async updateTableCounts() {
        try {
            const requestVersion = this._nextCountVersion();
            // Fetch cached counts
            const response = await fetch('/api/dashboard/counts');
            if (!response.ok) return;

            const data = await response.json();

            const updatedMasters = new Set();
            const updatedFacts = new Set();

            // Update master table counts
            for (const [tableName, tableInfo] of Object.entries(data.master_tables || {})) {
                const value = this.resolveCountValue(tableInfo);
                if (value !== null) {
                    this.setTableCount(tableName, value, null, { version: requestVersion });
                    updatedMasters.add(tableName);
                } else {
                    const countEl = document.getElementById(`count-${tableName}`);
                    const existing = countEl ? Number(countEl.dataset.countValue) : NaN;
                    if (!updatedMasters.has(tableName) && (Number.isNaN(existing) || existing <= 0)) {
                        this.setTableCount(
                            tableName,
                            null,
                            '0 records',
                            { version: requestVersion, updateVersionOnFallback: true }
                        );
                    }
                }
            }

            // Update fact table counts
            for (const [tableName, tableInfo] of Object.entries(data.fact_tables || {})) {
                const value = this.resolveCountValue(tableInfo);
                if (value !== null) {
                    this.setTableCount(tableName, value, null, { version: requestVersion });
                    updatedFacts.add(tableName);
                } else {
                    const countEl = document.getElementById(`count-${tableName}`);
                    const existing = countEl ? Number(countEl.dataset.countValue) : NaN;
                    if (!updatedFacts.has(tableName) && (Number.isNaN(existing) || existing <= 0)) {
                        this.setTableCount(
                            tableName,
                            null,
                            '0 records',
                            { version: requestVersion, updateVersionOnFallback: true }
                        );
                    }
                }
            }

            // Ensure tables without entries show zero
            this.masterTableNames.forEach(name => {
                if (!updatedMasters.has(name)) {
                    const countEl = document.getElementById(`count-${name}`);
                    const existing = countEl ? Number(countEl.dataset.countValue) : NaN;
                    if (Number.isNaN(existing) || existing <= 0) {
                        this.setTableCount(
                            name,
                            null,
                            '0 records',
                            { version: requestVersion, updateVersionOnFallback: true }
                        );
                    }
                }
            });

            this.factTableNames.forEach(name => {
                if (!updatedFacts.has(name)) {
                    const countEl = document.getElementById(`count-${name}`);
                    const existing = countEl ? Number(countEl.dataset.countValue) : NaN;
                    if (Number.isNaN(existing) || existing <= 0) {
                        this.setTableCount(
                            name,
                            null,
                            '0 records',
                            { version: requestVersion, updateVersionOnFallback: true }
                        );
                    }
                }
            });
        } catch (error) {
            console.error('Failed to update table counts:', error);
        }
    }

    async checkForActiveTasks() {
        try {
            // First, check localStorage for stored task
            const storedTask = localStorage.getItem('activeHistoricalTask');

            if (storedTask) {
                const { taskId, startTime } = JSON.parse(storedTask);

                // Verify task is still running on server
                const response = await fetch(`/api/tasks/${taskId}/status`);

                // If task not found (404) or any error, clear localStorage
                if (!response.ok) {
                    console.log('Task not found on server, clearing localStorage:', taskId);
                    localStorage.removeItem('activeHistoricalTask');
                    return;
                }

                const status = await response.json();

                // Only reconnect if task is actively running
                if (status.status === 'running' || status.status === 'pending') {
                    console.log('Found active historical task, reconnecting...', taskId);
                    this.reconnectToHistoricalTask(taskId);
                    return;
                } else {
                    // Task completed or failed, clear localStorage
                    console.log('Task no longer active, clearing localStorage:', status.status);
                    localStorage.removeItem('activeHistoricalTask');
                }
            }

            // Also check server for any active tasks (backup check)
            const activeResponse = await fetch('/api/tasks/active');

            if (activeResponse.ok) {
                const data = await activeResponse.json();

                // Look for historical generation tasks
                const historicalTask = data.active_tasks.find(task =>
                    task.task_id && task.task_id.includes('historical')
                );

                if (historicalTask) {
                    console.log('Found active historical task on server, reconnecting...', historicalTask.task_id);
                    this.reconnectToHistoricalTask(historicalTask.task_id);
                }
            }

        } catch (error) {
            console.error('Error checking for active tasks:', error);
            // Don't throw - this is a best-effort check
        }
    }

    async reconnectToHistoricalTask(taskId) {
        // First, switch to the historical tab so button is visible
        await this.switchTab('historical');

        // Wait for DOM to update
        await new Promise(resolve => setTimeout(resolve, 100));

        const generateBtn = document.querySelector('#historical button.btn.primary.large');

        // Verify button exists before proceeding
        if (!generateBtn) {
            console.error('Historical generate button not found after tab switch');
            return;
        }

        const originalHTML = generateBtn.innerHTML;

        try {
            // Update UI to show generation is active
            generateBtn.disabled = true;
            generateBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generating...';

            // Show progress section
            this.showProgress('historicalProgress', 'historicalProgressFill', 'historicalProgressText');

            // Store in localStorage if not already there
            if (!localStorage.getItem('activeHistoricalTask')) {
                localStorage.setItem('activeHistoricalTask', JSON.stringify({
                    taskId: taskId,
                    startTime: new Date().toISOString()
                }));
            }

            // Resume polling
            console.log('Resuming polling for task:', taskId);
            const finalStatus = await this.pollProgress(
                `/api/tasks/${taskId}/status`,
                'historicalProgressFill',
                'historicalProgressText'
            );

            // Handle completion
            if (finalStatus?.status === 'completed') {
                this.showNotification('Historical data generation completed!', 'success');
                await this.loadGenerationState();
                await this.updateDashboardStats();
                await this.updateTableCounts();
            } else if (finalStatus?.status === 'failed') {
                this.showNotification(`Generation failed: ${finalStatus.error_message || 'Unknown error'}`, 'error');
            }

        } catch (error) {
            console.error('Error reconnecting to task:', error);
            this.showNotification('Lost connection to generation task', 'error');
        } finally {
            // Clean up - only if button exists
            if (generateBtn) {
                generateBtn.disabled = false;
                generateBtn.innerHTML = originalHTML;
            }
            localStorage.removeItem('activeHistoricalTask');
            this.hideProgress('historicalProgress');
        }
    }

    async switchTab(tabName) {
        // Hide all tabs
        document.querySelectorAll('.tab-content').forEach(tab => {
            tab.classList.remove('active');
        });
        
        // Show selected tab
        document.getElementById(tabName).classList.add('active');
        
        // Update nav tabs
        document.querySelectorAll('.nav-tab').forEach(tab => {
            tab.classList.remove('active');
        });
        
        document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');
        
        this.currentTab = tabName;
        try { localStorage.setItem('activeTab', tabName); } catch (_) {}

        // Load tab-specific data
        if (tabName === 'streaming') {
            await this.updateStreamingStatus();
        } else if (tabName === 'dashboard') {
            await this.updateDashboardStats();
        } else if (tabName === 'master-data') {
            await this.updateTableCounts();
            await Promise.all(this.masterTableNames.map(name => this.ensureTableCount(name)));
            await this.updateAllTablesData();
        } else if (tabName === 'historical') {
            await this.updateTableCounts();
        }
    }

    setDefaultDates() {
        const today = new Date();
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(today.getDate() - 30);
        
        document.getElementById('startDate').value = thirtyDaysAgo.toISOString().split('T')[0];
        document.getElementById('endDate').value = today.toISOString().split('T')[0];
    }

    async generateMasterData() {
        // Reset table indicators before starting
        this.clearMasterTableStatuses();
        
        this.showProgress('masterDataProgress', 'masterProgressFill', 'masterProgressText');
        
        try {
            const response = await fetch('/api/generate/master', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({})
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const result = await response.json();
            
            // Poll for progress updates using the operation_id
            await this.pollProgress(`/api/generate/master/status?operation_id=${result.operation_id}`, 'masterProgressFill', 'masterProgressText');

            this.showNotification('Master data generation completed successfully!', 'success');
            await this.updateDashboardStats();
            await this.updateTableCounts();
            await this.updateAllTablesData();
            
        } catch (error) {
            console.error('Master data generation failed:', error);
            this.showNotification(`Master data generation failed: ${error.message}`, 'error');
        } finally {
            this.hideProgress('masterDataProgress');
        }
    }

    async generateHistoricalData() {
        // Get button reference
        const generateBtn = document.querySelector('#historical button.btn.primary.large');
        if (!generateBtn) {
            console.error('Generate button not found');
            return;
        }

        // Save original button state
        const originalHTML = generateBtn.innerHTML;
        const originalDisabled = generateBtn.disabled;

        // Disable button and show loading state
        generateBtn.disabled = true;
        generateBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generating...';

        try {
            const startDate = document.getElementById('startDate').value;
            const endDate = document.getElementById('endDate').value;
            const parallel = document.getElementById('parallelGeneration').checked;

            // Create request body - dates are optional now for intelligent date logic
            const requestBody = { parallel: parallel };

            // Always generate all fact tables; switches removed

            // Only include dates if manually specified
            if (startDate && endDate) {
                if (new Date(startDate) > new Date(endDate)) {
                    this.showNotification('Start date must be before end date', 'error');
                    return;
                }
                requestBody.start_date = startDate;
                requestBody.end_date = endDate;
            }

            // Debug logging for troubleshooting
            console.log('Historical data generation request body:', JSON.stringify(requestBody, null, 2));

            // Clear any previous table status indicators
            this.clearHistoricalTableStatuses();

            this.showProgress('historicalProgress', 'historicalProgressFill', 'historicalProgressText');

            // Retry logic for handling intermittent 400/422 errors
            let response;
            let lastError;
            const maxRetries = 3;

            for (let attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    console.log(`Historical data generation attempt ${attempt}/${maxRetries}...`);
                    console.log('Sending request body:', JSON.stringify(requestBody));

                    response = await fetch('/api/generate/historical', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(requestBody)
                    });

                    console.log(`Attempt ${attempt} response: ${response.status} ${response.statusText}`);

                    if (response.ok) {
                        console.log(`Attempt ${attempt} succeeded!`);
                        break; // Success, exit retry loop
                    } else if (response.status !== 400 && response.status !== 422) {
                        console.log(`Attempt ${attempt} failed with non-retryable error: ${response.status}`);
                        break; // Non-retryable error, exit retry loop
                    }

                    lastError = new Error(`HTTP ${response.status}: ${response.statusText}`);
                    if (attempt < maxRetries) {
                        console.warn(`Historical data generation attempt ${attempt} failed with ${response.status}, retrying in ${attempt}s...`);
                        await new Promise(resolve => setTimeout(resolve, 1000 * attempt)); // Exponential backoff
                        continue;
                    } else {
                        console.error(`All ${maxRetries} attempts failed. Last error: ${response.status} ${response.statusText}`);
                    }
                } catch (fetchError) {
                    console.error(`Attempt ${attempt} network error:`, fetchError);
                    lastError = fetchError;
                    if (attempt < maxRetries) {
                        console.warn(`Historical data generation attempt ${attempt} failed with network error, retrying in ${attempt}s...`);
                        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
                        continue;
                    } else {
                        console.error(`All ${maxRetries} attempts failed with network errors.`);
                    }
                }
            }

            if (!response.ok) {
                let msg = `HTTP ${response.status}: ${response.statusText}`;
                try {
                    const err = await response.json();
                    if (err && (err.detail || err.message)) {
                        msg = err.detail || err.message;
                    }
                } catch (_) { /* ignore */ }
                throw new Error(msg);
            }

            const result = await response.json();
            const taskId = result.operation_id;

            // Store task ID in localStorage for reconnection on refresh
            localStorage.setItem('activeHistoricalTask', JSON.stringify({
                taskId: taskId,
                startTime: new Date().toISOString()
            }));

            // Poll for progress updates using the NEW generic endpoint
            const finalStatus = await this.pollProgress(
                `/api/tasks/${taskId}/status`,  // FIXED URL - uses generic task status endpoint
                'historicalProgressFill',
                'historicalProgressText'
            );

            // Check actual status and show appropriate notification
            if (finalStatus && finalStatus.status === 'completed') {
                this.showNotification('Historical data generation completed successfully!', 'success');
            } else if (finalStatus && finalStatus.status === 'failed') {
                this.showNotification(
                    `Historical data generation failed: ${finalStatus.error_message || 'Unknown error'}`,
                    'error'
                );
            } else {
                this.showNotification('Historical data generation status unknown', 'warning');
            }

            await this.loadGenerationState();
            await this.updateDashboardStats();
            await this.updateTableCounts();
            await this.updateAllTablesData();

        } catch (error) {
            console.error('Historical data generation failed:', error);
            this.showNotification(`Historical data generation failed: ${error.message}`, 'error');
        } finally {
            // Always restore button state and clear localStorage
            generateBtn.disabled = originalDisabled;
            generateBtn.innerHTML = originalHTML;
            localStorage.removeItem('activeHistoricalTask');
            this.hideProgress('historicalProgress');
        }
    }

    async startStreaming() {
        const duration = document.getElementById('streamDuration').value;
        
        try {
            const requestBody = {};
            if (duration) {
                requestBody.duration_minutes = parseInt(duration);
            }
            
            const response = await fetch('/api/stream/start', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestBody)
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            this.showNotification('Streaming started successfully!', 'success');
            this.updateStreamingUI(true);
            this.startStreamingMonitor();
            
        } catch (error) {
            console.error('Failed to start streaming:', error);
            this.showNotification(`Failed to start streaming: ${error.message}`, 'error');
        }
    }

    async stopStreaming() {
        try {
            const response = await fetch('/api/stream/stop', {
                method: 'POST'
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            this.showNotification('Streaming stopped', 'info');
            this.updateStreamingUI(false);
            this.stopStreamingMonitor();
            
        } catch (error) {
            console.error('Failed to stop streaming:', error);
            this.showNotification(`Failed to stop streaming: ${error.message}`, 'error');
        }
    }

    async updateStreamingStatus() {
        try {
            const response = await fetch('/api/stream/status');
            const status = await response.json();
            
            this.updateStreamingUI(status.is_streaming);
            
            if (status.is_streaming) {
                document.getElementById('eventsSent').textContent = status.events_sent.toLocaleString();
                document.getElementById('eventsPerSecond').textContent = status.events_per_second.toFixed(1);
                document.getElementById('uptime').textContent = this.formatUptime(status.uptime_seconds);
                
                if (!this.streamingInterval) {
                    this.startStreamingMonitor();
                }
            } else {
                this.stopStreamingMonitor();
            }
            
        } catch (error) {
            console.error('Failed to get streaming status:', error);
        }

        // Also update disruptions if we're on streaming tab
        const currentTab = document.querySelector('.tab-content.active');
        if (currentTab && currentTab.id === 'streaming') {
            await this.loadActiveDisruptions();
        }
    }

    updateStreamingUI(isStreaming) {
        const status = document.getElementById('streamStatus');
        const startBtn = document.getElementById('startStreamBtn');
        const stopBtn = document.getElementById('stopStreamBtn');
        
        if (isStreaming) {
            status.innerHTML = '<i class="fas fa-play-circle text-green"></i><span>Streaming</span>';
            status.className = 'stream-status streaming';
            startBtn.disabled = true;
            stopBtn.disabled = false;
        } else {
            status.innerHTML = '<i class="fas fa-stop-circle text-red"></i><span>Stopped</span>';
            status.className = 'stream-status stopped';
            startBtn.disabled = false;
            stopBtn.disabled = true;
        }
    }

    startStreamingMonitor() {
        this.streamingInterval = setInterval(() => {
            this.updateStreamingStatus();
            this.loadRecentEvents();
        }, 2000);
    }

    stopStreamingMonitor() {
        if (this.streamingInterval) {
            clearInterval(this.streamingInterval);
            this.streamingInterval = null;
        }
    }

    async loadRecentEvents() {
        try {
            const response = await fetch('/api/stream/events/recent');
            const events = await response.json();
            
            const eventLog = document.getElementById('eventLog');
            if (events.length > 0) {
                eventLog.innerHTML = events.map(event => 
                    `<div>[${new Date(event.timestamp).toLocaleTimeString()}] ${event.event_type}: ${JSON.stringify(event.summary)}</div>`
                ).join('');
                eventLog.scrollTop = eventLog.scrollHeight;
            }
            
        } catch (error) {
            console.error('Failed to load recent events:', error);
        }
    }

    async saveConfig() {
        // Validate required fields
        const requiredFields = [
            { id: 'stores', name: 'Number of Stores', min: 1 },
            { id: 'dcs', name: 'Distribution Centers', min: 1 },
            { id: 'totalGeographies', name: 'Total Geographies', min: 10 },
            { id: 'totalCustomers', name: 'Total Customers', min: 1000 },
            { id: 'totalProducts', name: 'Total Products', min: 100 },
            { id: 'customersPerDay', name: 'Customers per Day', min: 100 },
            { id: 'itemsPerTicket', name: 'Items per Ticket', min: 0.1 },
            { id: 'emitInterval', name: 'Emit Interval', min: 100 },
            { id: 'burstSize', name: 'Burst Size', min: 1 },
            { id: 'hubName', name: 'Hub Name', type: 'string' }
        ];

        for (const field of requiredFields) {
            const element = document.getElementById(field.id);
            const value = element.value.trim();
            
            if (!value) {
                this.showNotification(`${field.name} is required`, 'error');
                element.focus();
                return;
            }
            
            if (field.type !== 'string') {
                const numValue = parseFloat(value);
                if (isNaN(numValue) || numValue < field.min) {
                    this.showNotification(`${field.name} must be at least ${field.min}`, 'error');
                    element.focus();
                    return;
                }
            }
        }

        const config = {
            seed: 42, // Keep existing seed
            volume: {
                stores: parseInt(document.getElementById('stores').value),
                dcs: parseInt(document.getElementById('dcs').value),
                total_geographies: parseInt(document.getElementById('totalGeographies').value),
                total_customers: parseInt(document.getElementById('totalCustomers').value),
                total_products: parseInt(document.getElementById('totalProducts').value),
                customers_per_day: parseInt(document.getElementById('customersPerDay').value),
                items_per_ticket_mean: parseFloat(document.getElementById('itemsPerTicket').value),
                online_orders_per_day: parseInt(document.getElementById('onlineOrdersPerDay').value),
                marketing_impressions_per_day: parseInt(document.getElementById('marketingImpressionsPerDay').value)
            },
            realtime: {
                emit_interval_ms: parseInt(document.getElementById('emitInterval').value),
                burst: parseInt(document.getElementById('burstSize').value)
            },
            paths: {
                dict: "data/dictionaries",
                master: "data/master", 
                facts: "data/facts"
            },
            stream: {
                hub: document.getElementById('hubName').value.trim()
            }
        };

        // Add Azure connection string if provided
        const connectionString = document.getElementById('connectionString').value.trim();
        if (connectionString) {
            config.realtime.azure_connection_string = connectionString;
        }
        
        try {
            const response = await fetch('/api/config', {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(config)
            });
            
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
            }
            
            this.showNotification('Configuration saved successfully!', 'success');
            
        } catch (error) {
            console.error('Failed to save configuration:', error);
            this.showNotification(`Failed to save configuration: ${error.message}`, 'error');
        }
    }

    async resetConfig() {
        if (!confirm('Are you sure you want to reset all configuration to defaults? This will overwrite your current settings.')) {
            return;
        }
        
        try {
            const response = await fetch('/api/config/reset', {
                method: 'POST'
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            await this.loadConfiguration();
            this.showNotification('Configuration reset to defaults', 'success');
            
        } catch (error) {
            console.error('Failed to reset configuration:', error);
            this.showNotification(`Failed to reset configuration: ${error.message}`, 'error');
        }
    }

    async testConfig() {
        // Build config object same as saveConfig but don't save
        const config = {
            seed: 42,
            volume: {
                stores: parseInt(document.getElementById('stores').value),
                dcs: parseInt(document.getElementById('dcs').value),
                total_geographies: parseInt(document.getElementById('totalGeographies').value),
                total_customers: parseInt(document.getElementById('totalCustomers').value),
                total_products: parseInt(document.getElementById('totalProducts').value),
                customers_per_day: parseInt(document.getElementById('customersPerDay').value),
                items_per_ticket_mean: parseFloat(document.getElementById('itemsPerTicket').value),
                marketing_impressions_per_day: parseInt(document.getElementById('marketingImpressionsPerDay').value)
            },
            realtime: {
                emit_interval_ms: parseInt(document.getElementById('emitInterval').value),
                burst: parseInt(document.getElementById('burstSize').value)
            },
            paths: {
                dict: "data/dictionaries",
                master: "data/master", 
                facts: "data/facts"
            },
            stream: {
                hub: document.getElementById('hubName').value.trim()
            }
        };

        // Add Azure connection string if provided
        const connectionString = document.getElementById('connectionString').value.trim();
        if (connectionString) {
            config.realtime.azure_connection_string = connectionString;
        }
        
        try {
            const response = await fetch('/api/config/validate', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(config)
            });
            
            const result = await response.json();
            
            if (result.valid) {
                this.showNotification('Configuration is valid! ✅', 'success');
            } else {
                this.showNotification(`Configuration validation failed: ${result.message}`, 'error');
            }
            
        } catch (error) {
            console.error('Failed to test configuration:', error);
            this.showNotification(`Failed to test configuration: ${error.message}`, 'error');
        }
    }

    async previewTable(tableName, tableType = 'Master Data') {
        const modal = document.getElementById('previewModal');
        const title = document.getElementById('previewTitle');
        const content = document.getElementById('previewContent');

        title.textContent = `Preview: ${tableName.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}`;
        content.innerHTML = '<div class="loading-text">Loading preview...</div>';

        modal.style.display = 'block';

        try {
            let response;
            if (tableType === 'Historical Data') {
                // For fact tables, get recent data
                response = await fetch(`/api/facts/${tableName}/recent?limit=100`);
            } else {
                // For master tables
                response = await fetch(`/api/master/${tableName}`);
            }

            if (!response.ok) {
                throw new Error('Table not found or not generated yet');
            }

            const result = await response.json();
            const data = result.preview_rows || [];

            if (data.length === 0) {
                content.innerHTML = '<div class="text-center">No data available. Generate data first.</div>';
                return;
            }

            // Create table HTML (suppress counts in preview)
            const headers = result.columns || Object.keys(data[0]);
            const tableHtml = `
                <div class="table-container">
                    ${tableType === 'Historical Data' && result.most_recent_date ?
                        `<p class="preview-note">Most recent partition: ${result.most_recent_date}</p>` : ''}
                    <table class="preview-table">
                        <thead>
                            <tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr>
                        </thead>
                        <tbody>
                            ${data.slice(0, 10).map(row =>
                                `<tr>${headers.map(h => `<td>${row[h] || ''}</td>`).join('')}</tr>`
                            ).join('')}
                        </tbody>
                    </table>
                </div>
            `;
            
            content.innerHTML = tableHtml;
            
        } catch (error) {
            console.error('Failed to preview table:', error);
            content.innerHTML = `<div class="error-text">Failed to load preview: ${error.message}</div>`;
        }
    }

    closePreview() {
        document.getElementById('previewModal').style.display = 'none';
    }

    async pollProgress(statusUrl, progressFillId, progressTextId) {
        const maxAttempts = 600; // 10 minutes max
        let attempts = 0;

        return new Promise((resolve) => {
            const isMasterProgress = progressFillId === 'masterProgressFill';
            const isHistoricalProgress = progressFillId === 'historicalProgressFill';

            const poll = async () => {
                if (attempts >= maxAttempts) {
                    console.error('Polling timeout after 10 minutes');
                    resolve({ status: 'failed', message: 'Polling timeout' });
                    return;
                }
                attempts++;

                try {
                    const response = await fetch(statusUrl);
                    if (!response.ok) {
                        console.error('Polling failed:', response.status, response.statusText, 'url:', statusUrl);
                        // Fallback for older endpoint if generic task endpoint not found
                        if (response.status === 404 && statusUrl.includes('/api/tasks/')) {
                            const operationId = statusUrl.split('/api/tasks/')[1]?.split('/status')[0];
                            if (operationId) {
                                const legacyUrl = `/api/generate/historical/status?operation_id=${operationId}`;
                                console.warn('Retrying poll with legacy endpoint:', legacyUrl);
                                const legacyResp = await fetch(legacyUrl);
                                if (legacyResp.ok) {
                                    const status = await legacyResp.json();
                                    console.log('[Progress Poll] Legacy Status received:', status);
                                    // Normalize legacy payload to current shape
                                    status.progress = status.progress || 0;
                                    proceedWithStatus(status);
                                    setTimeout(poll, 500);
                                    return;
                                }
                            }
                        }
                        setTimeout(poll, 500);
                        return;
                    }

                    const status = await response.json();
                    console.log('[Progress Poll] Status received:', status);

                    const proceedWithStatus = (statusObj) => {
                        // Update progress bar
                        const progressFill = document.getElementById(progressFillId);
                        const progressText = document.getElementById(progressTextId);
                        console.log('[Progress Poll] Elements found:', {
                            progressFillId,
                            progressFill: !!progressFill,
                            progressTextId,
                            progressText: !!progressText
                        });

                        if (progressFill && progressText) {
                            const progress = Math.round((statusObj.progress || 0) * 100);
                            console.log('[Progress Poll] Updating UI - Progress:', progress, 'Message:', statusObj.message);
                            progressFill.style.width = `${progress}%`;
                            progressText.textContent = statusObj.message || `${progress}%`;
                        } else {
                            console.error('[Progress Poll] Elements not found! Cannot update UI.');
                        }

                        // Update per-table status indicators
                        if (statusObj.table_progress) {
                            for (const [tableName, tableProgress] of Object.entries(statusObj.table_progress)) {
                                if (tableProgress >= 1.0) {
                                    this.updateTableStatus(tableName, 'completed');
                                } else if (tableProgress > 0) {
                                    this.updateTableStatus(tableName, 'processing');
                                }
                            }
                        }

                        if (statusObj.tables_completed) {
                            statusObj.tables_completed.forEach(table => this.updateTableStatus(table, 'completed'));
                        }

                        if (statusObj.tables_in_progress) {
                            statusObj.tables_in_progress.forEach(table => {
                                const progressValue = statusObj.table_progress?.[table];
                                if (!progressValue || progressValue < 1.0) {
                                    this.updateTableStatus(table, 'processing');
                                }
                            });
                        }

                        if (statusObj.tables_remaining) {
                            statusObj.tables_remaining.forEach(table => {
                                if (!statusObj.table_progress || !(table in statusObj.table_progress)) {
                                    this.updateTableStatus(table, null);
                                }
                            });
                        }

                        // Mark failed tables if any
                        if (statusObj.status === 'failed' && statusObj.tables_failed) {
                            statusObj.tables_failed.forEach(table => this.updateTableStatus(table, 'failed'));
                        }

                        if (isHistoricalProgress) {
                            this.updateHistoricalProgress(statusObj);
                            this.updateHourlyProgress(statusObj);
                        } else if (isMasterProgress) {
                            this.updateMasterProgress(statusObj);
                        }

                        const tablesToRefresh = new Set();
                        (statusObj.tables_in_progress || []).forEach(table => tablesToRefresh.add(table));
                        (statusObj.tables_completed || []).forEach(table => tablesToRefresh.add(table));
                        if (statusObj.table_progress) {
                            Object.entries(statusObj.table_progress).forEach(([table, value]) => {
                                if (value >= 0.99) {
                                    tablesToRefresh.add(table);
                                }
                            });
                        }
                        tablesToRefresh.forEach(table => this.maybeRefreshTableCount(table));

                        if (statusObj.table_counts) {
                            Object.entries(statusObj.table_counts).forEach(([table, count]) => {
                                if (typeof count === 'number') {
                                    const version = this._nextCountVersion();
                                    this.setTableCount(table, count, null, { version });
                                }
                            });
                        }

                        // Check if done
                        if (statusObj.status === 'completed' || statusObj.status === 'failed') {
                            if (isHistoricalProgress) {
                                this.hideTableCounter('tableProgressCounter');
                                this.hideETA('progressETA');
                                this.clearProgressDetails('historicalProgressDetails');
                                this.hideHourlyProgress();
                            } else if (isMasterProgress) {
                                this.hideTableCounter('masterTableProgressCounter');
                                this.hideETA('masterProgressETA');
                                this.clearProgressDetails('masterProgressDetails');
                            }
                            resolve(statusObj);
                            return;
                        }
                    };

                    proceedWithStatus(status);

                    // Wait before next poll
                    setTimeout(poll, 500);
                } catch (error) {
                    console.error('Progress polling error:', error);
                    // Retry on error
                    setTimeout(poll, 500);
                }
            };

            poll();
        });
    }

    updateTableCounter(status, elementId = 'tableProgressCounter') {
        const tablesCompleted = status.tables_completed?.length || 0;
        const tablesInProgress = status.tables_in_progress?.length || 0;
        const tablesRemaining = status.tables_remaining?.length || 0;
        const totalTables = tablesCompleted + tablesInProgress + tablesRemaining;

        const counterElement = document.getElementById(elementId);
        if (counterElement && totalTables > 0) {
            counterElement.textContent = `${tablesCompleted}/${totalTables} tables complete`;
            counterElement.style.display = 'block';
        }
    }

    hideTableCounter(elementId = 'tableProgressCounter') {
        const counterElement = document.getElementById(elementId);
        if (counterElement) {
            counterElement.style.display = 'none';
        }
    }

    updateETA(estimatedSeconds, elementId = 'progressETA') {
        const etaElement = document.getElementById(elementId);
        if (!etaElement || !estimatedSeconds || estimatedSeconds <= 0) {
            if (etaElement) etaElement.style.display = 'none';
            return;
        }

        const formatted = this.formatETA(estimatedSeconds);
        etaElement.textContent = `ETA: ${formatted}`;
        etaElement.style.display = 'block';
    }

    hideETA(elementId = 'progressETA') {
        const etaElement = document.getElementById(elementId);
        if (etaElement) {
            etaElement.style.display = 'none';
        }
    }

    updateHistoricalProgress(status) {
        this.updateTableCounter(status, 'tableProgressCounter');
        this.updateETA(status.estimated_seconds_remaining, 'progressETA');
        this.updateProgressDetails(status, 'historicalProgressDetails');
    }

    updateMasterProgress(status) {
        this.updateTableCounter(status, 'masterTableProgressCounter');
        this.updateETA(status.estimated_seconds_remaining, 'masterProgressETA');
        this.updateProgressDetails(status, 'masterProgressDetails');
    }

    updateProgressDetails(status, detailsId) {
        const detailsElement = document.getElementById(detailsId);
        if (!detailsElement) return;

        const sections = [];

        if (status.tables_in_progress?.length) {
            sections.push(`<strong>In progress:</strong> ${this.formatTableList(status.tables_in_progress)}`);
        }

        if (status.tables_completed?.length) {
            sections.push(`<strong>Completed:</strong> ${this.formatTableList(status.tables_completed)}`);
        }

        if (status.tables_remaining?.length) {
            sections.push(`<strong>Remaining:</strong> ${this.formatTableList(status.tables_remaining)}`);
        }

        if (status.tables_failed?.length) {
            sections.push(`<strong>Failed:</strong> ${this.formatTableList(status.tables_failed)}`);
        }

        detailsElement.innerHTML = sections.join('<br>');
    }

    updateHourlyProgress(status) {
        // Display current day and hour
        if (status.current_day !== undefined && status.current_hour !== undefined) {
            const hourDisplay = document.getElementById('currentHourDisplay');
            if (hourDisplay) {
                hourDisplay.textContent = `Day ${status.current_day}, Hour ${status.current_hour + 1}/24`;
                hourDisplay.style.display = 'block';
            }
        }

        // Display total hours completed
        if (status.total_hours_completed !== undefined) {
            const hoursDisplay = document.getElementById('totalHoursDisplay');
            if (hoursDisplay) {
                hoursDisplay.textContent = `${status.total_hours_completed} hours completed`;
                hoursDisplay.style.display = 'block';
            }
        }

        // Display per-table hourly progress
        if (status.hourly_progress && Object.keys(status.hourly_progress).length > 0) {
            const tableProgressContainer = document.getElementById('tableProgressContainer');
            if (tableProgressContainer) {
                let html = '<div class="table-progress-list">';
                for (const [table, progress] of Object.entries(status.hourly_progress)) {
                    const percentage = (progress * 100).toFixed(1);
                    html += `
                        <div class="table-progress-item">
                            <span class="table-name">${this.formatTableName(table)}</span>
                            <div class="table-progress-bar-container">
                                <div class="table-progress-bar" style="width: ${percentage}%"></div>
                            </div>
                            <span class="table-percentage">${percentage}%</span>
                        </div>
                    `;
                }
                html += '</div>';
                tableProgressContainer.innerHTML = html;
                tableProgressContainer.style.display = 'block';
            }
        }
    }

    hideHourlyProgress() {
        const hourDisplay = document.getElementById('currentHourDisplay');
        const hoursDisplay = document.getElementById('totalHoursDisplay');
        const tableProgressContainer = document.getElementById('tableProgressContainer');

        if (hourDisplay) hourDisplay.style.display = 'none';
        if (hoursDisplay) hoursDisplay.style.display = 'none';
        if (tableProgressContainer) tableProgressContainer.style.display = 'none';
    }

    clearProgressDetails(detailsId) {
        const detailsElement = document.getElementById(detailsId);
        if (detailsElement) {
            detailsElement.innerHTML = '';
        }
    }

    formatTableList(tables) {
        return tables.map(name => this.formatTableName(name)).join(', ');
    }

    formatTableName(tableName) {
        return tableName
            .replace(/_/g, ' ')
            .replace(/\b\w/g, char => char.toUpperCase());
    }

    formatETA(seconds) {
        if (seconds < 60) {
            return `~${Math.ceil(seconds)} seconds`;
        } else if (seconds < 3600) {
            const minutes = Math.ceil(seconds / 60);
            return `~${minutes} minute${minutes > 1 ? 's' : ''}`;
        } else {
            const hours = Math.floor(seconds / 3600);
            const minutes = Math.ceil((seconds % 3600) / 60);
            return `~${hours}h ${minutes}m`;
        }
    }

    showProgress(sectionId, progressFillId, progressTextId) {
        const section = document.getElementById(sectionId);
        const progressFill = document.getElementById(progressFillId);
        const progressText = document.getElementById(progressTextId);

        if (section) section.style.display = 'block';
        if (progressFill) progressFill.style.width = '0%';
        if (progressText) progressText.textContent = 'Starting...';

        // Reset table counter and ETA
        if (sectionId === 'masterDataProgress') {
            this.hideTableCounter('masterTableProgressCounter');
            this.hideETA('masterProgressETA');
            this.clearProgressDetails('masterProgressDetails');
        } else if (sectionId === 'historicalProgress') {
            this.hideTableCounter('tableProgressCounter');
            this.hideETA('progressETA');
            this.clearProgressDetails('historicalProgressDetails');
            this.hideHourlyProgress();
        } else {
            this.hideTableCounter();
            this.hideETA();
        }
    }

    hideProgress(sectionId) {
        const section = document.getElementById(sectionId);
        if (section) section.style.display = 'none';

        if (sectionId === 'masterDataProgress') {
            this.hideTableCounter('masterTableProgressCounter');
            this.hideETA('masterProgressETA');
            this.clearProgressDetails('masterProgressDetails');
        } else if (sectionId === 'historicalProgress') {
            this.hideTableCounter('tableProgressCounter');
            this.hideETA('progressETA');
            this.clearProgressDetails('historicalProgressDetails');
            this.hideHourlyProgress();
        } else {
            this.hideTableCounter();
            this.hideETA();
        }
    }

    showNotification(message, type = 'info') {
        // Simple notification system - you could enhance this with a toast library
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <div class="notification-content">
                <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i>
                <span>${message}</span>
                <button onclick="this.parentElement.parentElement.remove()" class="notification-close">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        `;
        
        // Add notification styles if they don't exist
        if (!document.querySelector('#notification-styles')) {
            const styles = document.createElement('style');
            styles.id = 'notification-styles';
            styles.textContent = `
                .notification {
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    z-index: 1000;
                    min-width: 300px;
                    background: white;
                    border-radius: 8px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                    animation: slideIn 0.3s ease;
                }
                .notification-success { border-left: 4px solid var(--success-color); }
                .notification-error { border-left: 4px solid var(--danger-color); }
                .notification-info { border-left: 4px solid var(--primary-color); }
                .notification-content {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    padding: 16px;
                }
                .notification-close {
                    margin-left: auto;
                    background: none;
                    border: none;
                    cursor: pointer;
                    opacity: 0.6;
                }
                @keyframes slideIn {
                    from { transform: translateX(100%); }
                    to { transform: translateX(0); }
                }
            `;
            document.head.appendChild(styles);
        }
        
        document.body.appendChild(notification);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 5000);
    }

    formatUptime(seconds) {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = Math.floor(seconds % 60);
        return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }

    async clearAllData() {
        // Single confirmation dialog
        const confirmed = confirm(
            '⚠️ This will permanently delete ALL generated data, including:\n' +
            '• Master data (stores, customers, products)\n' +
            '• Historical fact data\n' +
            '• Generation state tracking\n\n' +
            'This action cannot be undone. Proceed?'
        );
        if (!confirmed) return;
        
        try {
            this.showNotification('Clearing all data...', 'info');
            
            const response = await fetch('/api/generation/clear', {
                method: 'DELETE'
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const result = await response.json();
            
            this.showNotification(result.message, 'success');
            this.hideProgress('masterDataProgress');
            this.hideProgress('historicalProgress');
            this._tableCountVersions = {};
            this._countVersionSeq = 0;
            this._lastCountRefresh = {};
            this.clearMasterTableStatuses();
            this.clearHistoricalTableStatuses();

            // Refresh the UI state
            await this.loadGenerationState();
            await this.updateDashboardStats();
            await this.updateTableCounts();
            await this.updateAllTablesData();

        } catch (error) {
            console.error('Data clearing failed:', error);
            this.showNotification(`Failed to clear data: ${error.message}`, 'error');
        }
    }

    // Supply Chain Disruption Methods
    async createDisruption() {
        const type = document.getElementById('disruptionType').value;
        const targetId = parseInt(document.getElementById('targetId').value);
        const duration = parseInt(document.getElementById('duration').value);
        const severity = parseFloat(document.getElementById('severity').value);

        if (!targetId || !duration) {
            this.showNotification('Please provide target ID and duration', 'error');
            return;
        }

        try {
            const response = await fetch('/api/disruption/create', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    disruption_type: type,
                    target_id: targetId,
                    duration_minutes: duration,
                    severity: severity
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            this.showNotification(result.message, 'success');
            
            // Clear form
            document.getElementById('targetId').value = '';
            document.getElementById('duration').value = '30';
            document.getElementById('severity').value = '0.5';
            
            // Refresh disruptions list
            await this.loadActiveDisruptions();

        } catch (error) {
            console.error('Disruption creation failed:', error);
            this.showNotification(`Failed to create disruption: ${error.message}`, 'error');
        }
    }

    async clearAllDisruptions() {
        if (!confirm('Clear all active disruptions?')) {
            return;
        }

        try {
            const response = await fetch('/api/disruption/clear-all', {
                method: 'POST'
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            this.showNotification(result.message, 'success');
            
            // Refresh disruptions list
            await this.loadActiveDisruptions();

        } catch (error) {
            console.error('Clear disruptions failed:', error);
            this.showNotification(`Failed to clear disruptions: ${error.message}`, 'error');
        }
    }

    async loadActiveDisruptions() {
        try {
            const response = await fetch('/api/disruption/list');
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            const container = document.getElementById('activeDisruptions');
            
            if (data.count === 0) {
                container.innerHTML = '<div class="disruption-placeholder">No active disruptions</div>';
                return;
            }

            const disruptions = data.disruptions.map(disruption => {
                const typeLabel = disruption.type.replace('_', ' ').toUpperCase();
                const timeRemaining = Math.ceil(disruption.time_remaining_minutes);
                const severityPercent = Math.round(disruption.severity * 100);
                
                return `
                    <div class="disruption-item" data-id="${disruption.disruption_id}">
                        <div class="disruption-header">
                            <span class="disruption-type">${typeLabel}</span>
                            <span class="disruption-severity">Severity: ${severityPercent}%</span>
                            <button class="btn-small danger" onclick="cancelDisruption('${disruption.disruption_id}')">
                                <i class="fas fa-times"></i>
                            </button>
                        </div>
                        <div class="disruption-details">
                            <span>Target: ${disruption.target_id}</span>
                            <span>Time: ${timeRemaining}m remaining</span>
                            <span>Events: ${disruption.events_affected}</span>
                        </div>
                    </div>
                `;
            }).join('');

            container.innerHTML = disruptions;

        } catch (error) {
            console.error('Failed to load disruptions:', error);
            document.getElementById('activeDisruptions').innerHTML = 
                '<div class="disruption-placeholder error">Failed to load disruptions</div>';
        }
    }

    async cancelDisruption(disruptionId) {
        try {
            const response = await fetch(`/api/disruption/${disruptionId}`, {
                method: 'DELETE'
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            this.showNotification(result.message, 'success');
            
            // Refresh disruptions list
            await this.loadActiveDisruptions();

        } catch (error) {
            console.error('Cancel disruption failed:', error);
            this.showNotification(`Failed to cancel disruption: ${error.message}`, 'error');
        }
    }
}

// Global functions for HTML onclick handlers
let app;

function switchTab(tabName) {
    void app.switchTab(tabName);
}

function generateMasterData() {
    app.generateMasterData();
}

function generateHistoricalData() {
    app.generateHistoricalData();
}

function startStreaming() {
    app.startStreaming();
}

function stopStreaming() {
    app.stopStreaming();
}

function saveConfig() {
    app.saveConfig();
}

function resetConfig() {
    app.resetConfig();
}

function testConfig() {
    app.testConfig();
}

function previewTable(tableName) {
    app.previewTable(tableName);
}

function closePreview() {
    app.closePreview();
}

function createDisruption() {
    app.createDisruption();
}

function clearAllDisruptions() {
    app.clearAllDisruptions();
}

function cancelDisruption(disruptionId) {
    app.cancelDisruption(disruptionId);
}

// Initialize the application
document.addEventListener('DOMContentLoaded', () => {
    app = new RetailDataGenerator();
});

// Add table preview styles
const tableStyles = document.createElement('style');
tableStyles.textContent = `
.table-container {
    overflow-x: auto;
}
.preview-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
}
.preview-table th,
.preview-table td {
    padding: 8px 12px;
    border: 1px solid var(--border-color);
    text-align: left;
}
.preview-table th {
    background: var(--background-color);
    font-weight: 600;
}
.preview-note {
    margin-top: 12px;
    color: var(--text-secondary);
    font-style: italic;
    text-align: center;
}
.error-text {
    color: var(--danger-color);
    text-align: center;
    padding: 20px;
}

/* Data tables summary styles */
.tables-summary {
    margin-top: 24px;
}

.data-tables-summary {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}

.data-tables-summary th {
    background: #f8f9fa;
    padding: 12px 16px;
    text-align: left;
    font-weight: 600;
    color: #495057;
    border-bottom: 2px solid #dee2e6;
}

.data-tables-summary td {
    padding: 12px 16px;
    border-bottom: 1px solid #dee2e6;
    vertical-align: middle;
}

.data-tables-summary tr:hover {
    background-color: #f8f9fa;
}

.data-tables-summary td i {
    margin-right: 8px;
    width: 16px;
    color: #6c757d;
}

.table-generated td {
    opacity: 1;
}

.table-not-generated td {
    opacity: 0.6;
}

.record-count {
    font-family: 'Courier New', monospace;
    font-weight: 600;
    text-align: right;
}

.status-badge {
    padding: 4px 8px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 500;
    text-transform: uppercase;
}

.status-success {
    background-color: #EAE4FF; /* light lavender */
    color: #5B2ECC;           /* rich purple text */
}

.status-pending {
    background-color: #F1F3F5; /* light gray */
    color: #6C757D;            /* gray text */
}

.loading-text {
    text-align: center;
    color: #6c757d;
    font-style: italic;
    padding: 20px;
}
`;
document.head.appendChild(tableStyles);
