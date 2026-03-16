const App = (() => {
  const state = {
    tg: null,
    initData: "",
    ready: false,
    currentView: "home",
    servers: [],
    currentServer: null,
    currentPlayer: null,
    currentPlayerServerOpen: false,
    intervals: {
      servers: null,
      server: null,
      player: null
    },
    pingCooldowns: new Map(),
    banLookup: null,
    ui: {}
  };

  const icons = {
    home: `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
        <path d="M3 10.2L12 3l9 7.2v10.3a.5.5 0 0 1-.5.5H14v-6h-4v6H3.5a.5.5 0 0 1-.5-.5V10.2Z" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/>
      </svg>
    `,
    servers: `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
        <ellipse cx="12" cy="6.5" rx="7.5" ry="3.5" stroke="currentColor" stroke-width="1.8"/>
        <path d="M4.5 6.5v5c0 1.9 3.36 3.5 7.5 3.5s7.5-1.6 7.5-3.5v-5" stroke="currentColor" stroke-width="1.8"/>
        <path d="M4.5 11.5v5c0 1.9 3.36 3.5 7.5 3.5s7.5-1.6 7.5-3.5v-5" stroke="currentColor" stroke-width="1.8"/>
      </svg>
    `,
    search: `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
        <circle cx="11" cy="11" r="6.5" stroke="currentColor" stroke-width="1.8"/>
        <path d="M16 16l5 5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
      </svg>
    `,
    ban: `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
        <circle cx="12" cy="12" r="8.5" stroke="currentColor" stroke-width="1.8"/>
        <path d="M8.5 8.5l7 7" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
      </svg>
    `,
    refresh: `
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none">
        <path d="M20 11a8 8 0 1 0 2 5.3" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
        <path d="M20 4v7h-7" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
    `,
    close: `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
        <path d="M6 6l12 12M18 6L6 18" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
      </svg>
    `
  };

  function init() {
    setupTelegram();
    mountShell();
    bindGlobalEvents();
    setView("home");
    boot();
  }

  function setupTelegram() {
    const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
    state.tg = tg;
    state.initData = tg?.initData || "";
    if (tg) {
      tg.ready();
      tg.expand();
      tg.setHeaderColor("#0b0f16");
      tg.setBackgroundColor("#0b0f16");
      state.ready = true;
    }
  }

  function mountShell() {
    const root = document.getElementById("app") || document.body;
    root.innerHTML = `
      <div class="macos-bg">
        <div class="macos-blob b1"></div>
        <div class="macos-blob b2"></div>
        <div class="macos-blob b3"></div>
        <div class="macos-noise"></div>
      </div>

      <div class="app-shell">
        <aside class="sidebar glass">
          <div class="brand">
            <div class="brand-logo">M</div>
            <div class="brand-copy">
              <div class="brand-title">Moderation Center</div>
              <div class="brand-subtitle">Telegram Mini App • Roblox</div>
            </div>
          </div>

          <div class="sidebar-nav">
            <button class="nav-btn active" data-view-btn="home">
              <div class="nav-btn-left">
                <div class="nav-icon">${icons.home}</div>
                <div class="nav-label">Главная</div>
              </div>
              <div class="nav-badge" id="badgeHome">•</div>
            </button>

            <button class="nav-btn" data-view-btn="servers">
              <div class="nav-btn-left">
                <div class="nav-icon">${icons.servers}</div>
                <div class="nav-label">Серверы</div>
              </div>
              <div class="nav-badge" id="badgeServers">0</div>
            </button>

            <button class="nav-btn" data-view-btn="search">
              <div class="nav-btn-left">
                <div class="nav-icon">${icons.search}</div>
                <div class="nav-label">Поиск игрока</div>
              </div>
              <div class="nav-badge">⌘F</div>
            </button>

            <button class="nav-btn" data-view-btn="ban">
              <div class="nav-btn-left">
                <div class="nav-icon">${icons.ban}</div>
                <div class="nav-label">Бан-панель</div>
              </div>
              <div class="nav-badge">⚑</div>
            </button>
          </div>

          <div class="sidebar-footer">
            <div class="server-health-card">
              <div class="server-health-title">Онлайн серверов</div>
              <div class="server-health-value" id="onlineServersValue">0</div>
              <div class="server-health-sub" id="onlinePlayersValue">0 игроков в сети</div>
            </div>

            <div class="user-card glass">
              <div class="user-avatar" id="sidebarUserAvatar"></div>
              <div class="user-meta">
                <div class="user-name" id="sidebarUserName">Загрузка...</div>
                <div class="user-role" id="sidebarUserRole">Проверка доступа</div>
              </div>
            </div>
          </div>
        </aside>

        <main class="main">
          <header class="topbar glass">
            <div class="topbar-left">
              <div class="page-kicker" id="pageKicker">Overview</div>
              <div class="page-title" id="pageTitle">Панель модерирования Roblox</div>
              <div class="page-subtitle" id="pageSubtitle">Серверы, поиск игроков, модерация и баны в живом режиме.</div>
            </div>

            <div class="topbar-right">
              <div class="pill">
                <span class="pill-dot"></span>
                <span id="lastSyncLabel">Ожидание синхронизации</span>
              </div>
              <button class="btn btn-secondary" id="topRefreshBtn">${icons.refresh} Обновить</button>
            </div>
          </header>

          <div class="content-stage">
            <section class="view active" data-view="home">
              <div class="grid hero">
                <div class="card hero-card glass span-4">
                  <div>
                    <div class="hero-label">Серверы в сети</div>
                    <div class="hero-value" id="homeServersValue">0</div>
                  </div>
                  <div class="hero-foot">
                    <span>Обновляется автоматически</span>
                    <span id="homePlayersSmall">0 игроков</span>
                  </div>
                </div>

                <div class="card hero-card glass span-4">
                  <div>
                    <div class="hero-label">Игроков онлайн</div>
                    <div class="hero-value" id="homePlayersValue">0</div>
                  </div>
                  <div class="hero-foot">
                    <span>Суммарно по всем серверам</span>
                    <span id="homeAverageTps">TPS: —</span>
                  </div>
                </div>

                <div class="card hero-card glass span-4">
                  <div>
                    <div class="hero-label">Средний uptime</div>
                    <div class="hero-value" id="homeUptimeValue">0 мин</div>
                  </div>
                  <div class="hero-foot">
                    <span>Стабильность серверов</span>
                    <span id="homeBestServer">Лучший TPS: —</span>
                  </div>
                </div>

                <div class="card hero-card glass span-8">
                  <div>
                    <div class="hero-label">Быстрые действия</div>
                    <div class="hero-value">Открывай нужный раздел без лишних переходов</div>
                  </div>
                  <div class="quick-actions">
                    <button class="btn btn-primary" data-jump-view="servers">Открыть серверы</button>
                    <button class="btn btn-secondary" data-jump-view="search">Найти игрока</button>
                    <button class="btn btn-danger" data-jump-view="ban">Бан-панель</button>
                  </div>
                </div>

                <div class="card hero-card glass span-4">
                  <div>
                    <div class="hero-label">Последнее обновление</div>
                    <div class="hero-value" id="homeSyncValue">—</div>
                  </div>
                  <div class="hero-foot">
                    <span>Live polling</span>
                    <span id="homeSyncAgo">ожидание</span>
                  </div>
                </div>
              </div>

              <div class="section glass">
                <div class="section-head">
                  <div class="section-title-group">
                    <div class="section-kicker">Live</div>
                    <div class="section-title">Последние серверы</div>
                    <div class="section-subtitle">Компактный обзор активных серверов.</div>
                  </div>
                  <button class="btn btn-secondary" data-jump-view="servers">Все серверы</button>
                </div>
                <div id="homeServersPreview" class="servers-grid"></div>
              </div>
            </section>

            <section class="view" data-view="servers">
              <div class="section glass">
                <div class="section-head">
                  <div class="section-title-group">
                    <div class="section-kicker">Servers</div>
                    <div class="section-title">Список серверов</div>
                    <div class="section-subtitle">JobId, online, uptime, TPS и первые игроки.</div>
                  </div>
                </div>

                <div class="toolbar">
                  <div class="toolbar-group">
                    <div class="pill"><span id="serversToolbarCount">0</span> серверов онлайн</div>
                    <div class="pill"><span id="serversToolbarPlayers">0</span> игроков</div>
                  </div>
                  <div class="toolbar-group">
                    <button class="btn btn-secondary" id="serversRefreshBtn">${icons.refresh} Обновить сейчас</button>
                  </div>
                </div>

                <div id="serversList" class="servers-grid"></div>
              </div>
            </section>

            <section class="view" data-view="search">
              <div class="section glass">
                <div class="section-head">
                  <div class="section-title-group">
                    <div class="section-kicker">Search</div>
                    <div class="section-title">Поиск игрока на серверах</div>
                    <div class="section-subtitle">Ищи по username или display name и открывай конкретный сервер.</div>
                  </div>
                </div>

                <div class="toolbar">
                  <div class="searchbar" style="flex:1; min-width:min(100%,420px);">
                    <div class="searchbar-icon">${icons.search}</div>
                    <input id="searchInput" placeholder="Введите username или display name" autocomplete="off" />
                    <button class="btn btn-primary" id="searchBtn">Найти</button>
                  </div>
                </div>

                <div id="searchResults" class="results-grid"></div>
              </div>
            </section>

            <section class="view" data-view="ban">
              <div class="section glass">
                <div class="section-head">
                  <div class="section-title-group">
                    <div class="section-kicker">Ban Control</div>
                    <div class="section-title">Бан-панель</div>
                    <div class="section-subtitle">Найди игрока по username, проверь профиль и выдай бан или разбан.</div>
                  </div>
                </div>

                <div class="toolbar">
                  <div class="searchbar" style="flex:1; min-width:min(100%,420px);">
                    <div class="searchbar-icon">${icons.ban}</div>
                    <input id="banLookupInput" placeholder="Введите username Roblox" autocomplete="off" />
                    <button class="btn btn-primary" id="banLookupBtn">Показать</button>
                  </div>
                </div>

                <div id="banProfileWrap" class="hidden">
                  <div id="banProfileCard" class="profile-card"></div>

                  <div style="height:16px"></div>

                  <div class="form-grid">
                    <div class="field span-6">
                      <div class="label">Длительность в днях</div>
                      <input class="input" id="banDaysInput" type="number" min="0" step="1" placeholder="0 = перманентно" />
                    </div>

                    <div class="field span-6">
                      <div class="label">Причина</div>
                      <input class="input" id="banReasonInput" placeholder="Укажите причину бана" />
                    </div>

                    <div class="field">
                      <label class="checkbox-row">
                        <input id="banAltsInput" type="checkbox" />
                        <span>Пометить попытку бана твинков/альтов</span>
                      </label>
                    </div>

                    <div class="field">
                      <div class="toolbar-group">
                        <button class="btn btn-danger" id="issueBanBtn">Забанить</button>
                        <button class="btn btn-success" id="issueUnbanBtn">Разбанить</button>
                      </div>
                    </div>
                  </div>
                </div>

                <div id="banEmptyState" class="list-state">
                  <div>
                    <div class="list-state-title">Никого не выбрано</div>
                    <div>Введите username Roblox и открой профиль для модерации.</div>
                  </div>
                </div>
              </div>
            </section>
          </div>
        </main>
      </div>

      <div class="modal-layer" id="modalLayer"></div>
      <div class="toast-wrap" id="toastWrap"></div>
    `;

    state.ui = {
      pageKicker: document.getElementById("pageKicker"),
      pageTitle: document.getElementById("pageTitle"),
      pageSubtitle: document.getElementById("pageSubtitle"),
      lastSyncLabel: document.getElementById("lastSyncLabel"),
      topRefreshBtn: document.getElementById("topRefreshBtn"),
      sidebarUserAvatar: document.getElementById("sidebarUserAvatar"),
      sidebarUserName: document.getElementById("sidebarUserName"),
      sidebarUserRole: document.getElementById("sidebarUserRole"),
      onlineServersValue: document.getElementById("onlineServersValue"),
      onlinePlayersValue: document.getElementById("onlinePlayersValue"),
      homeServersValue: document.getElementById("homeServersValue"),
      homePlayersValue: document.getElementById("homePlayersValue"),
      homePlayersSmall: document.getElementById("homePlayersSmall"),
      homeAverageTps: document.getElementById("homeAverageTps"),
      homeUptimeValue: document.getElementById("homeUptimeValue"),
      homeBestServer: document.getElementById("homeBestServer"),
      homeSyncValue: document.getElementById("homeSyncValue"),
      homeSyncAgo: document.getElementById("homeSyncAgo"),
      homeServersPreview: document.getElementById("homeServersPreview"),
      badgeServers: document.getElementById("badgeServers"),
      serversToolbarCount: document.getElementById("serversToolbarCount"),
      serversToolbarPlayers: document.getElementById("serversToolbarPlayers"),
      serversList: document.getElementById("serversList"),
      searchInput: document.getElementById("searchInput"),
      searchBtn: document.getElementById("searchBtn"),
      searchResults: document.getElementById("searchResults"),
      banLookupInput: document.getElementById("banLookupInput"),
      banLookupBtn: document.getElementById("banLookupBtn"),
      banProfileWrap: document.getElementById("banProfileWrap"),
      banProfileCard: document.getElementById("banProfileCard"),
      banDaysInput: document.getElementById("banDaysInput"),
      banReasonInput: document.getElementById("banReasonInput"),
      banAltsInput: document.getElementById("banAltsInput"),
      issueBanBtn: document.getElementById("issueBanBtn"),
      issueUnbanBtn: document.getElementById("issueUnbanBtn"),
      banEmptyState: document.getElementById("banEmptyState"),
      modalLayer: document.getElementById("modalLayer"),
      toastWrap: document.getElementById("toastWrap")
    };

    bindShellEvents();
    renderSidebarUser();
    renderHomeSkeleton();
    renderServersSkeleton();
    renderSearchEmpty();
  }

  function bindShellEvents() {
    document.querySelectorAll("[data-view-btn]").forEach(btn => {
      btn.addEventListener("click", () => setView(btn.dataset.viewBtn));
    });

    document.querySelectorAll("[data-jump-view]").forEach(btn => {
      btn.addEventListener("click", () => setView(btn.dataset.jumpView));
    });

    state.ui.topRefreshBtn.addEventListener("click", async () => {
      await loadServers(true);
    });

    document.getElementById("serversRefreshBtn").addEventListener("click", async () => {
      await loadServers(true);
    });

    state.ui.searchBtn.addEventListener("click", onSearch);
    state.ui.searchInput.addEventListener("keydown", e => {
      if (e.key === "Enter") onSearch();
    });

    state.ui.banLookupBtn.addEventListener("click", onBanLookup);
    state.ui.banLookupInput.addEventListener("keydown", e => {
      if (e.key === "Enter") onBanLookup();
    });

    state.ui.issueBanBtn.addEventListener("click", onIssueBan);
    state.ui.issueUnbanBtn.addEventListener("click", onIssueUnban);
  }

  function bindGlobalEvents() {
    document.addEventListener("click", e => {
      const closeBtn = e.target.closest("[data-close-modal]");
      if (closeBtn) {
        const isPlayer = closeBtn.closest(".modal[data-modal='player']");
        if (isPlayer) {
          closePlayerOnly();
        } else {
          closeModal();
        }
      }

      const serverOpenBtn = e.target.closest("[data-open-server]");
      if (serverOpenBtn) {
        openServerModal(serverOpenBtn.dataset.openServer);
      }

      const playerOpenBtn = e.target.closest("[data-open-player]");
      if (playerOpenBtn) {
        openPlayerModal(playerOpenBtn.dataset.jobId, Number(playerOpenBtn.dataset.openPlayer));
      }

      const pingBtn = e.target.closest("[data-player-ping]");
      if (pingBtn) {
        requestPlayerPing(pingBtn.dataset.jobId, Number(pingBtn.dataset.playerPing), pingBtn);
      }

      const kickBtn = e.target.closest("[data-action-kick]");
      if (kickBtn) {
        executePlayerAction(kickBtn.dataset.jobId, Number(kickBtn.dataset.actionKick), "kick");
      }

      const killBtn = e.target.closest("[data-action-kill]");
      if (killBtn) {
        executePlayerAction(killBtn.dataset.jobId, Number(killBtn.dataset.actionKill), "kill");
      }

      const banBtn = e.target.closest("[data-action-ban]");
      if (banBtn) {
        issueQuickBanFromPlayer();
      }

      const searchOpenBtn = e.target.closest("[data-search-open-server]");
      if (searchOpenBtn) {
        openServerModal(searchOpenBtn.dataset.searchOpenServer);
      }
    });

    document.addEventListener("keydown", e => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "f") {
        e.preventDefault();
        setView("search");
        state.ui.searchInput.focus();
      }
      if (e.key === "Escape") {
        closeModal();
      }
    });
  }

  async function boot() {
    renderTelegramProfile();
    await loadServers(false);
    startServerPolling();
  }

  function renderTelegramProfile() {
    const user = state.tg?.initDataUnsafe?.user;
    if (user) {
      state.ui.sidebarUserName.textContent = [user.first_name, user.last_name].filter(Boolean).join(" ") || user.username || "Moderator";
      state.ui.sidebarUserRole.textContent = user.username ? `@${user.username}` : "Telegram Moderator";
      const letter = (user.first_name || user.username || "M").trim().charAt(0).toUpperCase();
      state.ui.sidebarUserAvatar.innerHTML = `<div style="width:100%;height:100%;display:grid;place-items:center;background:linear-gradient(135deg,rgba(110,168,255,.92),rgba(123,108,255,.88));font-weight:800;font-size:18px;">${escapeHtml(letter)}</div>`;
    } else {
      state.ui.sidebarUserName.textContent = "Local Preview";
      state.ui.sidebarUserRole.textContent = "Открой внутри Telegram";
      state.ui.sidebarUserAvatar.innerHTML = `<div style="width:100%;height:100%;display:grid;place-items:center;background:linear-gradient(135deg,rgba(110,168,255,.92),rgba(123,108,255,.88));font-weight:800;font-size:18px;">M</div>`;
    }
  }

  function renderSidebarUser() {
    state.ui.sidebarUserName.textContent = "Загрузка...";
    state.ui.sidebarUserRole.textContent = "Проверка доступа";
    state.ui.sidebarUserAvatar.innerHTML = `<div class="skeleton" style="width:100%;height:100%;border-radius:14px;"></div>`;
  }

  function setView(view) {
    state.currentView = view;
    document.querySelectorAll("[data-view]").forEach(v => {
      v.classList.toggle("active", v.dataset.view === view);
    });
    document.querySelectorAll("[data-view-btn]").forEach(v => {
      v.classList.toggle("active", v.dataset.viewBtn === view);
    });

    const viewMeta = {
      home: {
        kicker: "Overview",
        title: "Панель модерирования Roblox",
        subtitle: "Сводка по серверам, live обновления и быстрый переход к модерации."
      },
      servers: {
        kicker: "Servers",
        title: "Активные серверы",
        subtitle: "Смотри список живых серверов, uptime, TPS и игроков на каждом JobId."
      },
      search: {
        kicker: "Search",
        title: "Поиск игрока на серверах",
        subtitle: "Ищи по display name или username и быстро открывай нужный сервер."
      },
      ban: {
        kicker: "Ban Control",
        title: "Управление банами",
        subtitle: "Профиль игрока Roblox, выдача бана и разбан из одного окна."
      }
    };

    const meta = viewMeta[view];
    state.ui.pageKicker.textContent = meta.kicker;
    state.ui.pageTitle.textContent = meta.title;
    state.ui.pageSubtitle.textContent = meta.subtitle;
  }

  function startServerPolling() {
    clearInterval(state.intervals.servers);
    state.intervals.servers = setInterval(() => {
      loadServers(false);
    }, 15000);
  }

  function startCurrentServerPolling(jobId) {
    clearInterval(state.intervals.server);
    state.intervals.server = setInterval(() => {
      if (state.currentServer?.jobId === jobId) {
        refreshServerModal(jobId, false);
      }
    }, 8000);
  }

  function startCurrentPlayerPolling(jobId, userId) {
    clearInterval(state.intervals.player);
    state.intervals.player = setInterval(() => {
      if (state.currentPlayer && state.currentPlayer.jobId === jobId && state.currentPlayer.userId === userId) {
        refreshPlayerModal(jobId, userId, false);
      }
    }, 5000);
  }

  async function api(path, options = {}) {
    const headers = new Headers(options.headers || {});
    headers.set("Accept", "application/json");

    const hasBody = options.body !== undefined && options.body !== null;
    if (hasBody && !(options.body instanceof FormData)) {
      headers.set("Content-Type", "application/json");
    }
    if (state.initData) {
      headers.set("X-Telegram-Init-Data", state.initData);
    }

    const res = await fetch(path, {
      method: options.method || "GET",
      headers,
      body: hasBody ? (options.body instanceof FormData ? options.body : JSON.stringify(options.body)) : undefined,
      credentials: "same-origin"
    });

    const contentType = res.headers.get("content-type") || "";
    const payload = contentType.includes("application/json") ? await res.json().catch(() => ({})) : await res.text().catch(() => "");

    if (!res.ok) {
      const message = typeof payload === "object" && payload ? payload.error || payload.message || "Request failed" : "Request failed";
      throw new Error(message);
    }

    return payload;
  }

  async function loadServers(showToastFlag = false) {
    try {
      const data = await api("/api/servers");
      const servers = Array.isArray(data?.servers) ? data.servers.map(normalizeServer) : [];
      state.servers = servers.sort((a, b) => b.playerCount - a.playerCount);
      renderSummary();
      renderHome();
      renderServers();
      updateSyncTime();
      if (showToastFlag) {
        showToast("Обновлено", "Список серверов синхронизирован.");
      }
    } catch (err) {
      renderErrorIfEmpty();
      showToast("Ошибка загрузки", err.message || "Не удалось получить данные серверов.");
    }
  }

  function normalizeServer(raw) {
    const jobId = raw.job_id || raw.jobId || "";
    const playerCount = Number(raw.player_count ?? raw.playerCount ?? 0);
    const tps = Number(raw.tps ?? raw.server_tps ?? 0);
    const startedAt = raw.started_at || raw.startedAt || null;
    const uptimeMinutes = Number(raw.uptime_minutes ?? raw.uptimeMinutes ?? computeUptimeMinutes(startedAt));
    const firstPlayersRaw = raw.first_players || raw.firstPlayers || [];
    const firstPlayers = Array.isArray(firstPlayersRaw) ? firstPlayersRaw.slice(0, 5).map(normalizePlayer) : [];
    return {
      jobId,
      playerCount,
      tps,
      startedAt,
      uptimeMinutes,
      firstPlayers
    };
  }

  function normalizePlayer(raw) {
    const username = raw.username || raw.user_name || raw.name || "Unknown";
    const displayName = raw.display_name || raw.displayName || username;
    const avatarUrl = raw.avatar_url || raw.avatarUrl || defaultAvatar();
    return {
      userId: Number(raw.user_id ?? raw.userId ?? 0),
      username,
      displayName,
      accountAge: Number(raw.account_age ?? raw.accountAge ?? 0),
      deaths: Number(raw.deaths ?? 0),
      coins: Number(raw.coins ?? 0),
      ping: raw.ping ?? raw.lastPingMs ?? null,
      avatarUrl
    };
  }

  function renderSummary() {
    const serverCount = state.servers.length;
    const players = state.servers.reduce((sum, s) => sum + s.playerCount, 0);
    const avgTps = serverCount ? (state.servers.reduce((sum, s) => sum + (Number.isFinite(s.tps) ? s.tps : 0), 0) / serverCount) : 0;
    const avgUptime = serverCount ? Math.round(state.servers.reduce((sum, s) => sum + s.uptimeMinutes, 0) / serverCount) : 0;
    const bestServer = state.servers.slice().sort((a, b) => b.tps - a.tps)[0];

    state.ui.badgeServers.textContent = String(serverCount);
    state.ui.onlineServersValue.textContent = String(serverCount);
    state.ui.onlinePlayersValue.textContent = `${players} игроков в сети`;
    state.ui.homeServersValue.textContent = String(serverCount);
    state.ui.homePlayersValue.textContent = String(players);
    state.ui.homePlayersSmall.textContent = `${players} игроков`;
    state.ui.homeAverageTps.textContent = `TPS: ${avgTps ? avgTps.toFixed(1) : "—"}`;
    state.ui.homeUptimeValue.textContent = `${avgUptime} мин`;
    state.ui.homeBestServer.textContent = bestServer ? `Лучший TPS: ${bestServer.tps.toFixed(1)}` : "Лучший TPS: —";
    state.ui.serversToolbarCount.textContent = String(serverCount);
    state.ui.serversToolbarPlayers.textContent = String(players);
  }

  function renderHomeSkeleton() {
    state.ui.homeServersPreview.innerHTML = `
      <div class="list-state">
        <div>
          <div class="list-state-title">Загрузка данных</div>
          <div>Ожидаем первый список серверов.</div>
        </div>
      </div>
    `;
  }

  function renderHome() {
    const preview = state.servers.slice(0, 3);
    if (!preview.length) {
      state.ui.homeServersPreview.innerHTML = `
        <div class="list-state">
          <div>
            <div class="list-state-title">Серверов пока нет</div>
            <div>Когда Roblox-серверы начнут отправлять heartbeat, они появятся здесь.</div>
          </div>
        </div>
      `;
      return;
    }
    state.ui.homeServersPreview.innerHTML = preview.map(renderServerCard).join("");
  }

  function renderServersSkeleton() {
    state.ui.serversList.innerHTML = `
      <div class="list-state">
        <div>
          <div class="list-state-title">Загрузка серверов</div>
          <div>Подтягиваем live список активных JobId.</div>
        </div>
      </div>
    `;
  }

  function renderServers() {
    if (!state.servers.length) {
      state.ui.serversList.innerHTML = `
        <div class="list-state">
          <div>
            <div class="list-state-title">Нет активных серверов</div>
            <div>Как только первый сервер Roblox отправит heartbeat, он появится в списке.</div>
          </div>
        </div>
      `;
      return;
    }
    state.ui.serversList.innerHTML = state.servers.map(renderServerCard).join("");
  }

  function renderServerCard(server) {
    const tpsClass = getTpsClass(server.tps);
    const previewPlayers = server.firstPlayers.slice(0, 5);
    const moreCount = Math.max(0, server.playerCount - previewPlayers.length);

    return `
      <div class="server-card glass">
        <div class="server-head">
          <div>
            <div class="server-title">Сервер ${escapeHtml(trimJobId(server.jobId))}</div>
            <div class="server-sub">${escapeHtml(server.jobId)}</div>
          </div>

          <div class="server-tps">
            <div class="server-tps-value ${tpsClass}">${formatNumber(server.tps, 1)}</div>
            <div class="server-tps-label">TPS</div>
          </div>
        </div>

        <div class="metrics">
          <div class="metric">
            <div class="metric-label">Игроков</div>
            <div class="metric-value">${server.playerCount}</div>
          </div>
          <div class="metric">
            <div class="metric-label">Uptime</div>
            <div class="metric-value">${server.uptimeMinutes} мин</div>
          </div>
          <div class="metric">
            <div class="metric-label">Статус</div>
            <div class="metric-value ${tpsClass}">${tpsLabel(server.tps)}</div>
          </div>
        </div>

        <div class="server-preview">
          <div class="server-preview-stack">
            ${previewPlayers.map(player => `
              <div class="server-preview-avatar" title="${escapeHtml(player.displayName)}">
                <img src="${escapeAttr(player.avatarUrl)}" alt="${escapeAttr(player.displayName)}">
              </div>
            `).join("")}
            ${moreCount > 0 ? `<div class="server-preview-more">+${moreCount}</div>` : ""}
          </div>
        </div>

        <div class="server-actions">
          <button class="btn btn-primary" data-open-server="${escapeAttr(server.jobId)}">Открыть сервер</button>
        </div>
      </div>
    `;
  }

  function renderSearchEmpty() {
    state.ui.searchResults.innerHTML = `
      <div class="list-state">
        <div>
          <div class="list-state-title">Введи ник игрока</div>
          <div>Поиск вернёт все серверы, на которых сейчас найден игрок.</div>
        </div>
      </div>
    `;
  }

  async function onSearch() {
    const q = state.ui.searchInput.value.trim();
    if (!q) {
      renderSearchEmpty();
      return;
    }

    state.ui.searchResults.innerHTML = `
      <div class="list-state">
        <div>
          <div class="list-state-title">Идёт поиск</div>
          <div>Проверяем все активные серверы.</div>
        </div>
      </div>
    `;

    try {
      const data = await api(`/api/search/players?q=${encodeURIComponent(q)}`);
      const results = Array.isArray(data?.results) ? data.results : [];

      if (!results.length) {
        state.ui.searchResults.innerHTML = `
          <div class="list-state">
            <div>
              <div class="list-state-title">Ничего не найдено</div>
              <div>Игрок с таким username или display name не найден на активных серверах.</div>
            </div>
          </div>
        `;
        return;
      }

      state.ui.searchResults.innerHTML = results.map(item => {
        const player = normalizePlayer(item.player || {});
        const server = normalizeServer(item.server || {});
        return `
          <div class="search-card">
            <div class="search-avatar">
              <img src="${escapeAttr(player.avatarUrl)}" alt="${escapeAttr(player.username)}">
            </div>

            <div class="search-meta">
              <div class="search-title">${escapeHtml(player.displayName)}</div>
              <div class="search-sub">@${escapeHtml(player.username)} • JobId: ${escapeHtml(trimJobId(server.jobId))}</div>
              <div class="search-tags">
                <div class="tag">Сервер: ${escapeHtml(trimJobId(server.jobId))}</div>
                <div class="tag">Игроков: ${server.playerCount}</div>
                <div class="tag ${getTpsClass(server.tps)}">TPS: ${formatNumber(server.tps, 1)}</div>
              </div>
            </div>

            <div>
              <button class="btn btn-primary" data-search-open-server="${escapeAttr(server.jobId)}">Открыть сервер</button>
            </div>
          </div>
        `;
      }).join("");
    } catch (err) {
      state.ui.searchResults.innerHTML = `
        <div class="list-state">
          <div>
            <div class="list-state-title">Ошибка поиска</div>
            <div>${escapeHtml(err.message || "Не удалось выполнить поиск.")}</div>
          </div>
        </div>
      `;
    }
  }

  async function onBanLookup() {
    const username = state.ui.banLookupInput.value.trim();
    if (!username) return;

    state.ui.banProfileWrap.classList.add("hidden");
    state.ui.banEmptyState.classList.remove("hidden");
    state.ui.banEmptyState.innerHTML = `
      <div>
        <div class="list-state-title">Загрузка профиля</div>
        <div>Получаем данные игрока Roblox.</div>
      </div>
    `;

    try {
      const data = await api(`/api/roblox/user?username=${encodeURIComponent(username)}`);
      const player = normalizePlayer(data?.user || {});
      const banInfo = data?.ban || null;
      state.banLookup = {
        ...player,
        banned: Boolean(banInfo?.active || data?.banned),
        banInfo
      };
      renderBanLookup();
    } catch (err) {
      state.banLookup = null;
      state.ui.banProfileWrap.classList.add("hidden");
      state.ui.banEmptyState.classList.remove("hidden");
      state.ui.banEmptyState.innerHTML = `
        <div>
          <div class="list-state-title">Игрок не найден</div>
          <div>${escapeHtml(err.message || "Проверь username и попробуй снова.")}</div>
        </div>
      `;
    }
  }

  function renderBanLookup() {
    if (!state.banLookup) return;

    const p = state.banLookup;
    state.ui.banProfileCard.innerHTML = `
      <div class="profile-avatar">
        <img src="${escapeAttr(p.avatarUrl)}" alt="${escapeAttr(p.username)}">
      </div>

      <div class="profile-meta">
        <div class="profile-title">${escapeHtml(p.displayName)}</div>
        <div class="profile-sub">@${escapeHtml(p.username)} • UserId: ${p.userId || "—"}</div>
        <div class="profile-tags">
          <div class="tag">Возраст: ${p.accountAge || 0}</div>
          <div class="tag ${p.banned ? "tps-low" : "tps-high"}">${p.banned ? "Бан активен" : "Не забанен"}</div>
        </div>
      </div>
    `;

    state.ui.banProfileWrap.classList.remove("hidden");
    state.ui.banEmptyState.classList.add("hidden");
  }

  async function onIssueBan() {
    if (!state.banLookup) return;

    const daysRaw = state.ui.banDaysInput.value.trim();
    const reason = state.ui.banReasonInput.value.trim();
    const banAlts = Boolean(state.ui.banAltsInput.checked);
    const days = daysRaw === "" ? 0 : Math.max(0, Number(daysRaw));

    if (!reason) {
      showToast("Нужна причина", "Укажи причину бана.");
      return;
    }

    try {
      await api("/api/bans", {
        method: "POST",
        body: {
          user_id: state.banLookup.userId,
          username: state.banLookup.username,
          display_name: state.banLookup.displayName,
          avatar_url: state.banLookup.avatarUrl,
          days,
          reason,
          ban_alts: banAlts
        }
      });

      showToast("Бан выдан", `${state.banLookup.username} успешно забанен.`);
      state.banLookup.banned = true;
      renderBanLookup();
    } catch (err) {
      showToast("Не удалось выдать бан", err.message || "Ошибка бана.");
    }
  }

  async function onIssueUnban() {
    if (!state.banLookup?.userId) return;
    try {
      await api(`/api/bans/${state.banLookup.userId}`, {
        method: "DELETE"
      });
      showToast("Разбан выполнен", `${state.banLookup.username} успешно разбанен.`);
      state.banLookup.banned = false;
      renderBanLookup();
    } catch (err) {
      showToast("Не удалось разбанить", err.message || "Ошибка разбана.");
    }
  }

  async function openServerModal(jobId) {
    try {
      await refreshServerModal(jobId, true);
      startCurrentServerPolling(jobId);
    } catch (err) {
      showToast("Не удалось открыть сервер", err.message || "Сервер недоступен.");
    }
  }

  async function refreshServerModal(jobId, shouldRender = true) {
    const data = await api(`/api/servers/${encodeURIComponent(jobId)}`);
    const server = normalizeServer(data?.server || {});
    const playersRaw = Array.isArray(data?.players) ? data.players : [];
    const players = playersRaw.map(normalizePlayer);
    server.players = players;
    state.currentServer = server;

    if (shouldRender) {
      renderServerModal(server);
    } else {
      updateServerModal(server);
    }

    if (state.currentPlayer && state.currentPlayer.jobId === jobId) {
      const exists = players.some(p => p.userId === state.currentPlayer.userId);
      if (!exists) {
        showToast("Игрок вышел", "Карточка игрока закрыта, потому что игрок покинул сервер.");
        closePlayerOnly();
      }
    }
  }

  function renderServerModal(server) {
    const modalHtml = `
      <div class="modal-backdrop visible" data-modal-backdrop></div>
      <div class="modal glass visible" data-modal="server">
        <div class="modal-shell">
          <div class="modal-head">
            <div class="modal-title-wrap">
              <div class="modal-title">Сервер ${escapeHtml(trimJobId(server.jobId))}</div>
              <div class="modal-subtitle">${escapeHtml(server.jobId)}</div>
            </div>
            <button class="modal-close" data-close-modal>${icons.close}</button>
          </div>

          <div class="modal-body" id="serverModalBody">
            ${renderServerModalBody(server)}
          </div>
        </div>
      </div>
    `;
    state.ui.modalLayer.style.pointerEvents = "auto";
    state.ui.modalLayer.innerHTML = modalHtml;
    state.ui.modalLayer.querySelector("[data-modal-backdrop]").addEventListener("click", closeModal);
  }

  function updateServerModal(server) {
    const body = document.getElementById("serverModalBody");
    if (!body) return;
    body.innerHTML = renderServerModalBody(server);
  }

  function renderServerModalBody(server) {
    return `
      <div class="server-detail-grid">
        <div class="section glass" style="padding:16px;">
          <div class="section-head" style="margin-bottom:14px;">
            <div class="section-title-group">
              <div class="section-kicker">Players</div>
              <div class="section-title" style="font-size:20px;">Игроки на сервере</div>
              <div class="section-subtitle">Нажми на игрока, чтобы открыть карточку модерации.</div>
            </div>
          </div>

          <div class="player-list">
            ${
              server.players.length
                ? server.players.map(p => `
                  <button class="player-card" data-open-player="${p.userId}" data-job-id="${escapeAttr(server.jobId)}">
                    <div class="avatar">
                      <img src="${escapeAttr(p.avatarUrl)}" alt="${escapeAttr(p.username)}">
                    </div>
                    <div class="player-meta">
                      <div class="player-name">${escapeHtml(p.displayName)}</div>
                      <div class="player-sub">@${escapeHtml(p.username)}</div>
                      <div class="player-tags">
                        <div class="tag">Возраст: ${p.accountAge}</div>
                        <div class="tag">Монет: ${p.coins}</div>
                        <div class="tag">Смертей: ${p.deaths}</div>
                      </div>
                    </div>
                    <div class="btn btn-secondary">Открыть</div>
                  </button>
                `).join("")
                : `
                  <div class="list-state">
                    <div>
                      <div class="list-state-title">Игроков нет</div>
                      <div>Сервер активен, но список игроков пуст.</div>
                    </div>
                  </div>
                `
            }
          </div>
        </div>

        <div class="side-stack">
          <div class="info-card glass">
            <div class="info-title">Игроков</div>
            <div class="info-value">${server.playerCount}</div>
            <div class="info-sub">Онлайн прямо сейчас</div>
          </div>

          <div class="info-card glass">
            <div class="info-title">TPS</div>
            <div class="info-value ${getTpsClass(server.tps)}">${formatNumber(server.tps, 1)}</div>
            <div class="info-sub">${tpsLabel(server.tps)}</div>
          </div>

          <div class="info-card glass">
            <div class="info-title">Uptime</div>
            <div class="info-value">${server.uptimeMinutes} мин</div>
            <div class="info-sub">С момента запуска сервера</div>
          </div>

          <div class="info-card glass">
            <div class="info-title">JobId</div>
            <div class="info-value" style="font-size:18px;">${escapeHtml(trimJobId(server.jobId))}</div>
            <div class="info-sub">${escapeHtml(server.jobId)}</div>
          </div>
        </div>
      </div>
    `;
  }

  async function openPlayerModal(jobId, userId) {
    try {
      await refreshPlayerModal(jobId, userId, true);
      startCurrentPlayerPolling(jobId, userId);
    } catch (err) {
      showToast("Игрок недоступен", err.message || "Не удалось открыть карточку игрока.");
    }
  }

  async function refreshPlayerModal(jobId, userId, shouldRender = true) {
    const data = await api(`/api/servers/${encodeURIComponent(jobId)}/players/${userId}`);
    const player = normalizePlayer(data?.player || {});
    player.jobId = jobId;
    state.currentPlayer = player;

    if (shouldRender) {
      renderPlayerModal(player);
    } else {
      updatePlayerModal(player);
    }
  }

  function renderPlayerModal(player) {
    const existing = document.querySelector(".modal[data-modal='player']");
    const modalHtml = `
      <div class="modal glass visible" data-modal="player" style="z-index:3;">
        <div class="modal-shell">
          <div class="modal-head">
            <div class="modal-title-wrap">
              <div class="modal-title">Игрок ${escapeHtml(player.displayName)}</div>
              <div class="modal-subtitle">JobId: ${escapeHtml(player.jobId)}</div>
            </div>
            <button class="modal-close" data-close-modal>${icons.close}</button>
          </div>

          <div class="modal-body" id="playerModalBody">
            ${renderPlayerModalBody(player)}
          </div>
        </div>
      </div>
    `;

    if (existing) {
      existing.outerHTML = modalHtml;
    } else {
      state.ui.modalLayer.insertAdjacentHTML("beforeend", modalHtml);
    }
  }

  function updatePlayerModal(player) {
    const body = document.getElementById("playerModalBody");
    if (!body) return;
    body.innerHTML = renderPlayerModalBody(player);
  }

  function renderPlayerModalBody(player) {
    return `
      <div class="player-modal-grid">
        <div class="player-panel">
          <div class="player-overview glass">
            <div class="player-avatar-lg">
              <img src="${escapeAttr(player.avatarUrl)}" alt="${escapeAttr(player.username)}">
            </div>
            <div class="player-overview-meta">
              <div class="player-overview-name">${escapeHtml(player.displayName)}</div>
              <div class="player-overview-sub">@${escapeHtml(player.username)} • UserId: ${player.userId || "—"}</div>
              <div class="player-tags" style="margin-top:12px;">
                <div class="tag">Возраст: ${player.accountAge}</div>
                <div class="tag">Смертей: ${player.deaths}</div>
                <div class="tag">Монет: ${player.coins}</div>
              </div>
            </div>
          </div>

          <div class="stat-grid">
            <div class="stat-card glass">
              <div class="stat-key">Username</div>
              <div class="stat-val">${escapeHtml(player.username)}</div>
            </div>
            <div class="stat-card glass">
              <div class="stat-key">DisplayName</div>
              <div class="stat-val">${escapeHtml(player.displayName)}</div>
            </div>
            <div class="stat-card glass">
              <div class="stat-key">Возраст</div>
              <div class="stat-val">${player.accountAge}</div>
            </div>
            <div class="stat-card glass">
              <div class="stat-key">Пинг</div>
              <div class="stat-val">${player.ping !== null && player.ping !== undefined ? `${Number(player.ping).toFixed(0)} ms` : "—"}</div>
            </div>
            <div class="stat-card glass">
              <div class="stat-key">Смертей</div>
              <div class="stat-val">${player.deaths}</div>
            </div>
            <div class="stat-card glass">
              <div class="stat-key">Монет</div>
              <div class="stat-val">${player.coins}</div>
            </div>
            <div class="stat-card glass" style="grid-column:1 / -1;">
              <div class="stat-key">JobId сервера</div>
              <div class="stat-val" style="font-size:18px;">${escapeHtml(player.jobId)}</div>
            </div>
          </div>
        </div>

        <div class="player-panel">
          <div class="action-grid">
            <div class="action-card glass">
              <div class="action-title">Быстрые действия</div>
              <div class="action-sub">Выполнение идёт через очередь команд Roblox-серверу.</div>
              <div class="action-row">
                <button class="btn btn-secondary" data-player-ping="${player.userId}" data-job-id="${escapeAttr(player.jobId)}">Узнать пинг</button>
                <button class="btn btn-warning" data-action-kill="${player.userId}" data-job-id="${escapeAttr(player.jobId)}">Убить</button>
                <button class="btn btn-danger" data-action-kick="${player.userId}" data-job-id="${escapeAttr(player.jobId)}">Кикнуть</button>
              </div>
            </div>

            <div class="action-card glass">
              <div class="action-title">Забанить игрока</div>
              <div class="action-sub">Укажи срок в днях, причину и отправь бан прямо из карточки игрока.</div>
              <div class="player-quick-ban">
                <div class="field">
                  <div class="label">Длительность в днях</div>
                  <input class="input" id="quickBanDaysInput" type="number" min="0" step="1" placeholder="0 = перманентно" />
                </div>

                <div class="field">
                  <div class="label">Причина</div>
                  <textarea class="textarea" id="quickBanReasonInput" placeholder="Укажи причину бана"></textarea>
                </div>

                <div class="field">
                  <label class="checkbox-row">
                    <input id="quickBanAltsInput" type="checkbox" />
                    <span>Пометить попытку бана твинков/альтов</span>
                  </label>
                </div>

                <div class="field">
                  <button class="btn btn-danger" data-action-ban="${player.userId}" data-job-id="${escapeAttr(player.jobId)}">Забанить</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  async function requestPlayerPing(jobId, userId, button) {
    const key = `${jobId}:${userId}`;
    const until = state.pingCooldowns.get(key) || 0;
    if (Date.now() < until) {
      const left = Math.ceil((until - Date.now()) / 1000);
      showToast("Подожди немного", `Пинг можно узнать снова через ${left} сек.`);
      return;
    }

    try {
      button.disabled = true;
      button.textContent = "Запрос...";
      await api(`/api/servers/${encodeURIComponent(jobId)}/players/${userId}/ping`, {
        method: "POST"
      });
      state.pingCooldowns.set(key, Date.now() + 5000);
      showToast("Запрос отправлен", "Сервер получил команду измерить пинг игрока.");
      setTimeout(() => {
        refreshPlayerModal(jobId, userId, false).catch(() => {});
      }, 1500);
    } catch (err) {
      showToast("Ошибка запроса пинга", err.message || "Не удалось запросить пинг.");
    } finally {
      button.disabled = false;
      button.textContent = "Узнать пинг";
    }
  }

  async function executePlayerAction(jobId, userId, actionType) {
    const labels = {
      kick: "Кикнуть",
      kill: "Убить"
    };

    try {
      await api("/api/actions", {
        method: "POST",
        body: {
          job_id: jobId,
          user_id: userId,
          action_type: actionType,
          payload: {}
        }
      });
      showToast("Команда поставлена в очередь", `${labels[actionType]}: команда отправлена Roblox-серверу.`);
    } catch (err) {
      showToast("Команда не отправлена", err.message || "Не удалось поставить действие в очередь.");
    }
  }

  async function issueQuickBanFromPlayer() {
    if (!state.currentPlayer) return;

    const daysField = document.getElementById("quickBanDaysInput");
    const reasonField = document.getElementById("quickBanReasonInput");
    const altsField = document.getElementById("quickBanAltsInput");
    const days = Math.max(0, Number(daysField?.value || 0));
    const reason = (reasonField?.value || "").trim();
    const banAlts = Boolean(altsField?.checked);

    if (!reason) {
      showToast("Нужна причина", "Укажи причину бана для игрока.");
      return;
    }

    try {
      await api("/api/bans", {
        method: "POST",
        body: {
          user_id: state.currentPlayer.userId,
          username: state.currentPlayer.username,
          display_name: state.currentPlayer.displayName,
          avatar_url: state.currentPlayer.avatarUrl,
          days,
          reason,
          ban_alts: banAlts,
          job_id: state.currentPlayer.jobId
        }
      });
      showToast("Игрок забанен", `${state.currentPlayer.username} успешно забанен.`);
    } catch (err) {
      showToast("Бан не выдан", err.message || "Не удалось выдать бан.");
    }
  }

  function closeModal() {
    clearInterval(state.intervals.server);
    clearInterval(state.intervals.player);
    state.currentServer = null;
    state.currentPlayer = null;
    state.ui.modalLayer.innerHTML = "";
    state.ui.modalLayer.style.pointerEvents = "none";
  }

  function closePlayerOnly() {
    clearInterval(state.intervals.player);
    state.currentPlayer = null;
    const playerModal = document.querySelector(".modal[data-modal='player']");
    if (playerModal) playerModal.remove();
  }

  function updateSyncTime() {
    const now = new Date();
    const text = now.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    state.ui.lastSyncLabel.textContent = `Синхронизация ${text}`;
    state.ui.homeSyncValue.textContent = text;
    state.ui.homeSyncAgo.textContent = "только что";
  }

  function renderErrorIfEmpty() {
    if (!state.servers.length) {
      state.ui.homeServersPreview.innerHTML = `
        <div class="list-state">
          <div>
            <div class="list-state-title">Ошибка связи</div>
            <div>Не удалось получить данные серверов с backend.</div>
          </div>
        </div>
      `;
      state.ui.serversList.innerHTML = `
        <div class="list-state">
          <div>
            <div class="list-state-title">Ошибка связи</div>
            <div>Проверь backend и повтори обновление.</div>
          </div>
        </div>
      `;
    }
  }

  function showToast(title, subtitle = "") {
    const el = document.createElement("div");
    el.className = "toast";
    el.innerHTML = `
      <div class="toast-title">${escapeHtml(title)}</div>
      ${subtitle ? `<div class="toast-sub">${escapeHtml(subtitle)}</div>` : ""}
    `;
    state.ui.toastWrap.appendChild(el);
    requestAnimationFrame(() => el.classList.add("visible"));
    setTimeout(() => {
      el.classList.remove("visible");
      setTimeout(() => el.remove(), 280);
    }, 3200);
  }

  function computeUptimeMinutes(startedAt) {
    if (!startedAt) return 0;
    const time = new Date(startedAt).getTime();
    if (Number.isNaN(time)) return 0;
    return Math.max(0, Math.floor((Date.now() - time) / 60000));
  }

  function trimJobId(jobId) {
    if (!jobId) return "—";
    return jobId.length > 16 ? `${jobId.slice(0, 8)}…${jobId.slice(-6)}` : jobId;
  }

  function tpsLabel(tps) {
    if (tps >= 16) return "Стабильно";
    if (tps >= 10) return "Средне";
    return "Низко";
  }

  function getTpsClass(tps) {
    if (tps >= 16) return "tps-high";
    if (tps >= 10) return "tps-mid";
    return "tps-low";
  }

  function formatNumber(value, digits = 0) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "—";
    return num.toFixed(digits);
  }

  function defaultAvatar() {
    return "https://tr.rbxcdn.com/30DAY-AvatarHeadshot-4201A8BF6F2B95E6A2FD6B1F2C9428F0-Png/150/150/AvatarHeadshot/Webp/noFilter";
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }

  return { init };
})();

document.addEventListener("DOMContentLoaded", App.init);
