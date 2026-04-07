import "dotenv/config";
import {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonInteraction,
  ButtonStyle,
  ChatInputCommandInteraction,
  Client,
  DiscordAPIError,
  EmbedBuilder,
  Events,
  GatewayIntentBits,
  Interaction,
  MessageFlags,
  PermissionFlagsBits,
  REST,
  Routes,
  SlashCommandBuilder,
  StringSelectMenuBuilder,
  StringSelectMenuInteraction,
} from "discord.js";
import { promises as fs } from "node:fs";
import path from "node:path";

type Account = {
  username: string;
  password: string;
  gameId: string;
};

type RentalRecord = {
  username: string;
  borrowedBy: string;
  borrowedAt: string;
  reminderSentAt?: string;
};

type AppState = {
  panelChannelId?: string;
  panelMessageId?: string;
  rentals: RentalRecord[];
  history: HistoryRecord[];
};

type HistoryAction = "borrow" | "return";

type HistoryRecord = {
  action: HistoryAction;
  username: string;
  displayName: string;
  userId: string;
  timestamp: string;
};

type RankDetails = {
  tierName: string;
  rr?: number;
  lastChange?: number;
  leaderboardRank?: number;
  peakTierName?: string;
};

type RankQueryResult = {
  details: RankDetails | null;
  error?: string;
};

type RankCacheEntry = RankQueryResult & {
  fetchedAt: number;
};

type RankedAccountSnapshot = {
  sortedAccounts: Account[];
  rankMap: Map<string, RankQueryResult>;
};

type EmojiSnapshot = {
  fetchedAt: number;
  emojis: Map<string, string>;
};

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required.`);
  }

  return value;
}

const TOKEN = requireEnv("DISCORD_TOKEN");
const CLIENT_ID = requireEnv("DISCORD_CLIENT_ID");
const GUILD_ID = requireEnv("DISCORD_GUILD_ID");
const LOG_CHANNEL_ID = process.env.LOG_CHANNEL_ID;
const HENRIK_API_KEYS = [process.env.HENRIK_API_KEY, process.env.HENRIK_API_KEY_2].filter(
  (value): value is string => Boolean(value),
);
const VALORANT_REGION = process.env.VALORANT_REGION ?? "ap";
const VALORANT_PLATFORM = process.env.VALORANT_PLATFORM ?? "pc";

const ACCOUNTS_FILE = path.resolve("accounts.txt");
const STATE_FILE = path.resolve("state.json");
const EMOJI_FILES_DIR = path.resolve("files");
const HENRIK_FETCH_TIMEOUT_MS = 15000;
const RANK_CACHE_TTL_MS = 10 * 60 * 1000;
const BORROW_REMINDER_DELAY_MS = 6 * 60 * 60 * 1000;
const BORROW_REMINDER_CHECK_INTERVAL_MS = 5 * 60 * 1000;

const rankCache = new Map<string, RankCacheEntry>();
let stateOperationQueue: Promise<void> = Promise.resolve();
let latestRankedSnapshot: RankedAccountSnapshot | null = null;
let emojiSnapshot: EmojiSnapshot | null = null;
const EMOJI_CACHE_TTL_MS = 30 * 60 * 1000;
let reminderCheckInProgress = false;

const client = new Client({
  intents: [GatewayIntentBits.Guilds],
});

const commands = [
  new SlashCommandBuilder()
    .setName("panel")
    .setDescription("このチャンネルに貸し出しパネルを作成または更新します。")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),
  new SlashCommandBuilder()
    .setName("account-add")
    .setDescription("accounts.txt にアカウントを追加します。")
    .addStringOption((option) =>
      option.setName("username").setDescription("ユーザー名").setRequired(true),
    )
    .addStringOption((option) =>
      option.setName("password").setDescription("パスワード").setRequired(true),
    )
    .addStringOption((option) =>
      option.setName("gameid").setDescription("gameid").setRequired(true),
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),
  new SlashCommandBuilder()
    .setName("upload-emojis")
    .setDescription("files フォルダから未登録の絵文字だけをこのGuildにアップロードします。")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),
  new SlashCommandBuilder()
    .setName("history")
    .setDescription("各アカウントを最後に使った人を表示します。")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),
].map((command) => command.toJSON());

async function ensureState(): Promise<AppState> {
  try {
    const raw = await fs.readFile(STATE_FILE, "utf8");
    const parsed = JSON.parse(raw) as Partial<AppState>;

    return {
      panelChannelId: parsed.panelChannelId,
      panelMessageId: parsed.panelMessageId,
      rentals: Array.isArray(parsed.rentals)
        ? parsed.rentals.map((rental) => ({
            username: rental.username ?? "",
            borrowedBy: rental.borrowedBy ?? "",
            borrowedAt: rental.borrowedAt ?? "",
            reminderSentAt:
              typeof rental.reminderSentAt === "string" ? rental.reminderSentAt : undefined,
          }))
        : [],
      history: Array.isArray(parsed.history) ? parsed.history : [],
    };
  } catch (error) {
    const nextState: AppState = { rentals: [], history: [] };
    await saveState(nextState);
    return nextState;
  }
}

async function saveState(state: AppState): Promise<void> {
  await fs.writeFile(STATE_FILE, JSON.stringify(state, null, 2), "utf8");
}

async function withStateLock<T>(operation: () => Promise<T>): Promise<T> {
  const previous = stateOperationQueue;
  let release!: () => void;
  stateOperationQueue = new Promise<void>((resolve) => {
    release = resolve;
  });

  await previous;

  try {
    return await operation();
  } finally {
    release();
  }
}

async function updateState<T>(operation: (state: AppState) => Promise<T>): Promise<T> {
  return withStateLock(async () => {
    const state = await ensureState();
    const result = await operation(state);
    await saveState(state);
    return result;
  });
}

async function loadAccounts(): Promise<Account[]> {
  const raw = await fs.readFile(ACCOUNTS_FILE, "utf8");

  return raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"))
    .map((line, index) => {
      const parts = line.split(":");
      if (parts.length !== 3) {
        throw new Error(`accounts.txt ${index + 1}行目の形式が不正です。username:password:gameid を使ってください。`);
      }

      const [username, password, gameId] = parts;
      return { username, password, gameId };
    });
}

async function appendAccount(account: Account): Promise<void> {
  const line = `${account.username}:${account.password}:${account.gameId}\n`;
  await fs.appendFile(ACCOUNTS_FILE, line, "utf8");
}

function addHistoryRecord(state: AppState, record: HistoryRecord): void {
  state.history.unshift(record);
  state.history = state.history.slice(0, 200);
}

async function sendLogMessage(content: string): Promise<void> {
  if (!LOG_CHANNEL_ID) {
    return;
  }

  try {
    const channel = await client.channels.fetch(LOG_CHANNEL_ID);
    if (channel?.isTextBased() && "send" in channel) {
      await channel.send(content);
    }
  } catch (error) {
    console.error("Failed to send log message:", error);
  }
}

async function sendTextMessage(channelId: string, content: string): Promise<boolean> {
  try {
    const channel = await client.channels.fetch(channelId);
    if (channel?.isTextBased() && "send" in channel) {
      await channel.send(content);
      return true;
    }
  } catch (error) {
    console.error("Failed to send text message:", error);
  }

  return false;
}

async function sendDirectMessage(userId: string, content: string): Promise<boolean> {
  try {
    const user = await client.users.fetch(userId);
    await user.send(content);
    return true;
  } catch (error) {
    console.error("Failed to send direct message:", error);
    return false;
  }
}

function runDetached(task: () => Promise<void>): void {
  void task().catch((error) => {
    console.error("Detached task failed:", error);
  });
}

function normalizeEmojiName(fileName: string): string {
  const baseName = path.parse(fileName).name.toLowerCase();
  const normalized = baseName.replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
  const clamped = normalized.slice(0, 32);

  if (clamped.length >= 2) {
    return clamped;
  }

  return `emoji_${Date.now().toString().slice(-6)}`;
}

async function uploadRankEmojis(): Promise<{
  uploaded: string[];
  skipped: string[];
  failed: string[];
}> {
  const guild = await client.guilds.fetch(GUILD_ID);
  await guild.emojis.fetch();

  const fileEntries = await fs.readdir(EMOJI_FILES_DIR, { withFileTypes: true });
  const imageFiles = fileEntries
    .filter((entry) => entry.isFile() && /\.(png|jpg|jpeg|gif)$/i.test(entry.name))
    .map((entry) => entry.name);

  const uploaded: string[] = [];
  const skipped: string[] = [];
  const failed: string[] = [];

  for (const fileName of imageFiles) {
    const emojiName = normalizeEmojiName(fileName);
    const imagePath = path.join(EMOJI_FILES_DIR, fileName);

    if (guild.emojis.cache.some((emoji) => emoji.name === emojiName)) {
      skipped.push(`${fileName} -> ${emojiName}`);
      continue;
    }

    try {
      await guild.emojis.create({
        attachment: imagePath,
        name: emojiName,
      });
      uploaded.push(`${fileName} -> ${emojiName}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown error";
      failed.push(`${fileName} -> ${emojiName} (${message})`);
    }
  }

  return { uploaded, skipped, failed };
}

function findRental(state: AppState, username: string): RentalRecord | undefined {
  return state.rentals.find((rental) => rental.username === username);
}

function findRentalByUser(state: AppState, userId: string): RentalRecord | undefined {
  return state.rentals.find((rental) => rental.borrowedBy === userId);
}

function formatRankText(details: RankDetails): string {
  return details.tierName;
}

function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength - 1)}…`;
}

function containsLineBreak(value: string): boolean {
  return /[\r\n]/.test(value);
}

function formatBorrowedAtRelative(isoDate: string): string {
  const unixSeconds = Math.floor(new Date(isoDate).getTime() / 1000);
  return `<t:${unixSeconds}:R>`;
}

function formatTimestampRelative(isoDate: string): string {
  const unixSeconds = Math.floor(new Date(isoDate).getTime() / 1000);
  return `<t:${unixSeconds}:R>`;
}

function formatTimestamp(isoDate: string): string {
  const unixSeconds = Math.floor(new Date(isoDate).getTime() / 1000);
  return `<t:${unixSeconds}:f>`;
}

function getDisplayName(account: Account): string {
  return account.gameId || account.username;
}

function isSameDisplayAndGameId(account: Account): boolean {
  return getDisplayName(account) === account.gameId;
}

function getRankSortValue(rankDetails: RankDetails | null | undefined): number {
  if (!rankDetails?.tierName) {
    return -1;
  }

  const normalized = rankDetails.tierName.toLowerCase();
  const order: Record<string, number> = {
    unranked: 0,
    iron: 1,
    bronze: 2,
    silver: 3,
    gold: 4,
    platinum: 5,
    diamond: 6,
    ascendant: 7,
    immortal: 8,
    radiant: 9,
  };

  const match = normalized.match(
    /^(radiant|immortal|ascendant|diamond|platinum|gold|silver|bronze|iron)(?:\s+([1-3]))?$/,
  );
  if (!match) {
    return -1;
  }

  const tier = match[1];
  const division = Number(match[2] ?? 0);
  return order[tier] * 10 + division;
}

async function sortAccountsByRank(
  accounts: Account[],
  options?: { forceRefresh?: boolean },
): Promise<RankedAccountSnapshot> {
  const rankEntries = await Promise.all(
    accounts.map(async (account) => [account.username, await getRankResult(account, options)] as const),
  );
  const rankMap = new Map(rankEntries);

  const sortedAccounts = [...accounts].sort((left, right) => {
    const leftRank = getRankSortValue(rankMap.get(left.username)?.details);
    const rightRank = getRankSortValue(rankMap.get(right.username)?.details);

    if (leftRank !== rightRank) {
      return rightRank - leftRank;
    }

    return getDisplayName(left).localeCompare(getDisplayName(right), "ja");
  });

  return { sortedAccounts, rankMap };
}

function formatRankForPanel(rankDetails: RankDetails | null, emojiMentionResolver: (emojiName: string) => string): string {
  const fallbackEmoji = emojiMentionResolver("unranked") || "Unrated";

  if (!rankDetails) {
    return fallbackEmoji;
  }

  const emoji = emojiMentionResolver(rankDetails.tierName);
  return emoji || fallbackEmoji;
}

async function getEmojiMentionMap(forceRefresh = false): Promise<Map<string, string>> {
  const now = Date.now();
  if (!forceRefresh && emojiSnapshot && now - emojiSnapshot.fetchedAt < EMOJI_CACHE_TTL_MS) {
    return emojiSnapshot.emojis;
  }

  const guild = await client.guilds.fetch(GUILD_ID);
  const emojis = await guild.emojis.fetch();
  const map = new Map<string, string>();

  for (const emoji of emojis.values()) {
    map.set(emoji.name, `<:${emoji.name}:${emoji.id}>`);
  }

  emojiSnapshot = {
    fetchedAt: now,
    emojis: map,
  };

  return map;
}

async function getRankResult(
  account: Account,
  options?: { forceRefresh?: boolean },
): Promise<RankQueryResult> {
  const forceRefresh = options?.forceRefresh ?? false;
  const cached = rankCache.get(account.username);
  const now = Date.now();

  if (!forceRefresh && cached && now - cached.fetchedAt < RANK_CACHE_TTL_MS) {
    return { details: cached.details, error: cached.error };
  }

  const result = await fetchValorantRankDetails(account);
  rankCache.set(account.username, {
    fetchedAt: now,
    details: result.details,
    error: result.error,
  });
  return result;
}

async function buildPanelEmbed(
  accounts: Account[],
  state: AppState,
  options?: { forceRefresh?: boolean },
): Promise<EmbedBuilder> {
  const emojis = await getEmojiMentionMap(options?.forceRefresh ?? false);
  const emojiMentionResolver = (emojiName: string): string => {
    const normalized = normalizeEmojiName(emojiName);
    return emojis.get(normalized) ?? "";
  };

  const { sortedAccounts, rankMap } = await sortAccountsByRank(accounts, options);
  latestRankedSnapshot = { sortedAccounts, rankMap };

  const usingNow = sortedAccounts
    .filter((account) => findRental(state, account.username))
    .map((account) => {
      const rental = findRental(state, account.username)!;
      const rankInfo = rankMap.get(account.username);
      const rankText = formatRankForPanel(rankInfo?.details ?? null, emojiMentionResolver);
      const displayName = truncate(getDisplayName(account), 28);
      return `${rankText} \`${displayName}\` ・ <@${rental.borrowedBy}> ・ ${formatBorrowedAtRelative(rental.borrowedAt)}`;
    });

  const available = sortedAccounts
    .filter((account) => !findRental(state, account.username))
    .map((account) => {
      const rankInfo = rankMap.get(account.username);
      const rankText = formatRankForPanel(rankInfo?.details ?? null, emojiMentionResolver);
      const displayName = truncate(getDisplayName(account), 28);
      return `${rankText} \`${displayName}\``;
    });

  const embed = new EmbedBuilder().setColor(0x2b2d31).setDescription(`空き ${available.length} ・ 利用中 ${usingNow.length}`);
  const availableFields = buildChunkedFields("利用可能", available.length > 0 ? available : ["なし"]);
  const usingFields = buildChunkedFields("貸出中", usingNow.length > 0 ? usingNow : ["なし"]);

  return embed.addFields([...availableFields, ...usingFields]);
}

function buildPanelComponents() {
  return [
    new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder()
        .setCustomId("borrow")
        .setLabel("借りる")
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId("return")
        .setLabel("返却する")
        .setStyle(ButtonStyle.Secondary),
      new ButtonBuilder()
        .setCustomId("refresh")
        .setLabel("更新")
        .setStyle(ButtonStyle.Success),
    ),
  ];
}

async function refreshPanel(): Promise<void> {
  await refreshPanelWithOptions();
}

async function refreshPanelWithOptions(options?: { forceRefresh?: boolean }): Promise<void> {
  const state = await ensureState();
  if (!state.panelChannelId || !state.panelMessageId) {
    return;
  }

  const channel = await client.channels.fetch(state.panelChannelId);
  if (!channel?.isTextBased()) {
    return;
  }

  const accounts = await loadAccounts();
  const message = await channel.messages.fetch(state.panelMessageId);
  await message.edit({
    embeds: [await buildPanelEmbed(accounts, state, options)],
    components: buildPanelComponents(),
  });
}

function buildBorrowMenu(accounts: Account[], state: AppState, userId: string) {
  const alreadyBorrowing = Boolean(findRentalByUser(state, userId));
  const available = accounts.filter((account) => !findRental(state, account.username));

  return new ActionRowBuilder<StringSelectMenuBuilder>().addComponents(
    new StringSelectMenuBuilder()
      .setCustomId("borrow-select")
      .setPlaceholder(
        alreadyBorrowing
          ? "返却するまで新しく借りられません"
          : available.length > 0
            ? "借りるアカウントを選択"
            : "利用可能なアカウントがありません",
      )
      .setDisabled(alreadyBorrowing || available.length === 0)
      .addOptions(
        (available.length > 0 ? available : [{ username: "none", gameId: "N/A", password: "" }]).map((account) => ({
          label: truncate(getDisplayName(account), 100),
          description: " ",
          value: account.username,
        })),
      ),
  );
}

function buildReturnMenu(accounts: Account[], state: AppState, userId: string) {
  const myAccounts = accounts.filter((account) => {
    const rental = findRental(state, account.username);
    return rental?.borrowedBy === userId;
  });

  return new ActionRowBuilder<StringSelectMenuBuilder>().addComponents(
    new StringSelectMenuBuilder()
      .setCustomId("return-select")
      .setPlaceholder(myAccounts.length > 0 ? "返却するアカウントを選択" : "返却可能なアカウントがありません")
      .setDisabled(myAccounts.length === 0)
      .addOptions(
        (myAccounts.length > 0 ? myAccounts : [{ username: "none", gameId: "N/A", password: "" }]).map((account) => ({
          label: truncate(getDisplayName(account), 100),
          description: " ",
          value: account.username,
        })),
      ),
  );
}

function buildHistorySummaryEmbed(accounts: Account[], history: HistoryRecord[]): EmbedBuilder {
  const lines = accounts.map((account) => {
    const latestBorrow = history.find(
      (entry) => entry.action === "borrow" && entry.username === account.username,
    );

    if (!latestBorrow) {
      return `\`${truncate(getDisplayName(account), 28)}\` ・ 履歴なし`;
    }

    return `\`${truncate(getDisplayName(account), 28)}\` ・ <@${latestBorrow.userId}> ・ ${formatTimestamp(latestBorrow.timestamp)}`;
  });

  const embed = new EmbedBuilder().setColor(0x2b2d31).setTitle("最終利用者一覧");
  const chunks = chunkLines(lines.length > 0 ? lines : ["履歴はありません。"], 3800);
  return embed.setDescription(chunks[0] ?? "履歴はありません。");
}

function chunkLines(lines: string[], maxLength: number): string[] {
  const chunks: string[] = [];
  let current = "";

  for (const line of lines) {
    const candidate = current.length === 0 ? line : `${current}\n${line}`;
    if (candidate.length > maxLength && current.length > 0) {
      chunks.push(current);
      current = line;
    } else {
      current = candidate;
    }
  }

  if (current.length > 0) {
    chunks.push(current);
  }

  return chunks;
}

function buildChunkedFields(name: string, lines: string[]): Array<{ name: string; value: string }> {
  const chunks = chunkLines(lines, 1000);
  return chunks.map((chunk, index) => ({
    name: index === 0 ? name : `${name} (${index + 1})`,
    value: chunk,
  }));
}

async function checkBorrowReminders(): Promise<void> {
  if (reminderCheckInProgress) {
    return;
  }

  reminderCheckInProgress = true;

  try {
    const state = await ensureState();
    const accounts = await loadAccounts();
    const accountMap = new Map(accounts.map((account) => [account.username, account]));
    const now = Date.now();
    const overdueRentals = state.rentals.filter((rental) => {
      if (rental.reminderSentAt) {
        return false;
      }

      const borrowedAt = new Date(rental.borrowedAt).getTime();
      return Number.isFinite(borrowedAt) && now - borrowedAt >= BORROW_REMINDER_DELAY_MS;
    });

    for (const rental of overdueRentals) {
      const account = accountMap.get(rental.username);
      const displayName = account ? getDisplayName(account) : rental.username;
      const content = `<@${rental.borrowedBy}> \`${displayName}\` を借りてから ${formatTimestampRelative(rental.borrowedAt)} です。返却を忘れていないか確認してください。`;
      const sent = await sendDirectMessage(
        rental.borrowedBy,
        `\`${displayName}\` を借りてから ${formatTimestampRelative(rental.borrowedAt)} です。返却を忘れていないか確認してください。`,
      );

      if (sent) {
        await updateState(async (latestState) => {
          const latestRental = latestState.rentals.find(
            (item) =>
              item.username === rental.username &&
              item.borrowedBy === rental.borrowedBy &&
              item.borrowedAt === rental.borrowedAt,
          );

          if (latestRental && !latestRental.reminderSentAt) {
            latestRental.reminderSentAt = new Date().toISOString();
          }
        });
      } else {
        runDetached(async () => {
          await sendLogMessage(`DM通知失敗 | \`${displayName}\` | <@${rental.borrowedBy}>`);
        });
      }
    }
  } finally {
    reminderCheckInProgress = false;
  }
}

function parseRiotId(gameId: string): { name: string; tag: string } | null {
  const [name, tag] = gameId.split("#");
  if (!name || !tag) {
    return null;
  }

  return { name, tag };
}

async function fetchValorantRank(account: Account): Promise<string> {
  const result = await fetchValorantRankDetails(account);
  if (result.error) {
    return result.error;
  }

  if (!result.details) {
    return "Unrated";
  }

  return formatRankText(result.details);
}

async function fetchValorantRankDetails(account: Account): Promise<{
  details: RankDetails | null;
  error?: string;
}> {
  if (HENRIK_API_KEYS.length === 0) {
    return { details: null, error: "HENRIK API key is missing." };
  }

  const riotId = parseRiotId(account.gameId);
  if (!riotId) {
    return {
      details: null,
      error: `Invalid gameid \`${account.gameId}\`. Use \`name#TAG\`.`,
    };
  }

  const url = new URL(
    `https://api.henrikdev.xyz/valorant/v3/mmr/${VALORANT_REGION}/${VALORANT_PLATFORM}/${encodeURIComponent(riotId.name)}/${encodeURIComponent(riotId.tag)}`,
  );

  let lastError = "Failed to fetch rank.";

  for (const apiKey of HENRIK_API_KEYS) {
    let response: Response;
    try {
      response = await fetch(url, {
        headers: {
          Authorization: apiKey,
          Accept: "application/json",
        },
        signal: AbortSignal.timeout(HENRIK_FETCH_TIMEOUT_MS),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "network error";
      lastError = `Failed to fetch rank: ${message}`;
      continue;
    }

    if (!response.ok) {
      let details = `${response.status} ${response.statusText}`;
      try {
        const errorJson = (await response.json()) as { errors?: Array<{ message?: string }> };
        const message = errorJson.errors?.[0]?.message;
        if (message) {
          details = `${details} / ${message}`;
        }
      } catch {
        // ignore parse failure and use status text
      }

      lastError = `Failed to fetch rank: ${details}`;
      continue;
    }

    const payload = (await response.json()) as {
      data?: {
        current?: {
          tier?: { name?: string };
          rr?: number;
          last_change?: number;
          leaderboard_placement?: { rank?: number };
        };
        peak?: {
          tier?: { name?: string };
        };
      };
    };

    const current = payload.data?.current;
    if (!current?.tier?.name) {
      return { details: null };
    }

    return {
      details: {
        tierName: current.tier.name,
        rr: current.rr,
        lastChange: current.last_change,
        leaderboardRank: current.leaderboard_placement?.rank,
        peakTierName: payload.data?.peak?.tier?.name,
      },
    };
  }

  return { details: null, error: lastError };
}

async function handlePanelCommand(interaction: ChatInputCommandInteraction): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  const accounts = await loadAccounts();
  const channel = interaction.channel;

  if (!channel?.isTextBased() || !("send" in channel) || !("messages" in channel)) {
    await interaction.editReply({ content: "テキストチャンネルで実行してください。" });
    return;
  }

  const result = await withStateLock(async () => {
    const state = await ensureState();
    const embed = await buildPanelEmbed(accounts, state);
    const components = buildPanelComponents();

    if (state.panelMessageId && state.panelChannelId === channel.id) {
      try {
        const message = await channel.messages.fetch(state.panelMessageId);
        await message.edit({ embeds: [embed], components });
        return "既存のパネルを更新しました。";
      } catch {
        // fall through and recreate panel
      }
    }

    const message = await channel.send({ embeds: [embed], components });
    state.panelChannelId = channel.id;
    state.panelMessageId = message.id;
    await saveState(state);
    return "パネルを作成しました。";
  });

  await interaction.editReply({ content: result });
}

async function handleAccountAddCommand(interaction: ChatInputCommandInteraction): Promise<void> {
  const username = interaction.options.getString("username", true).trim();
  const password = interaction.options.getString("password", true).trim();
  const gameId = interaction.options.getString("gameid", true).trim();

  if (
    [username, password, gameId].some(
      (value) => value.includes(":") || value.length === 0 || containsLineBreak(value),
    )
  ) {
    await interaction.reply({
      content: "username / password / gameid は空欄不可で、改行とコロン `:` は使えません。",
      flags: MessageFlags.Ephemeral,
    });
    return;
  }

  const accounts = await loadAccounts();
  if (accounts.some((account) => account.username === username)) {
    await interaction.reply({
      content: `\`${username}\` はすでに登録されています。`,
      flags: MessageFlags.Ephemeral,
    });
    return;
  }

  await appendAccount({ username, password, gameId });
  await refreshPanel();

  await interaction.reply({
    content: `\`${username}\` を accounts.txt に追加しました。`,
    flags: MessageFlags.Ephemeral,
  });
}

async function handleUploadRankEmojisCommand(
  interaction: ChatInputCommandInteraction,
): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  const result = await uploadRankEmojis();
  const summary = [
    `アップロード完了: ${result.uploaded.length}件`,
    `スキップ: ${result.skipped.length}件`,
    `失敗: ${result.failed.length}件`,
  ];

  if (result.uploaded.length > 0) {
    summary.push(`uploaded: ${result.uploaded.join(", ")}`);
  }

  if (result.skipped.length > 0) {
    summary.push(`skipped: ${result.skipped.join(", ")}`);
  }

  if (result.failed.length > 0) {
    summary.push(`failed: ${result.failed.join(", ")}`);
  }

  await interaction.editReply({
    content: summary.join("\n"),
  });
}

async function handleHistoryCommand(interaction: ChatInputCommandInteraction): Promise<void> {
  const state = await ensureState();
  const sortedAccounts =
    latestRankedSnapshot?.sortedAccounts ?? (await sortAccountsByRank(await loadAccounts())).sortedAccounts;
  const lines = sortedAccounts.map((account) => {
    const latestBorrow = state.history.find(
      (entry) => entry.action === "borrow" && entry.username === account.username,
    );

    if (!latestBorrow) {
      return `\`${truncate(getDisplayName(account), 28)}\` ・ 履歴なし`;
    }

    return `\`${truncate(getDisplayName(account), 28)}\` ・ <@${latestBorrow.userId}> ・ ${formatTimestamp(latestBorrow.timestamp)}`;
  });

  const chunks = chunkLines(lines.length > 0 ? lines : ["履歴はありません。"], 3800);
  const embeds = chunks.slice(0, 10).map((chunk, index) =>
    new EmbedBuilder()
      .setColor(0x2b2d31)
      .setTitle(index === 0 ? "最終利用者一覧" : `最終利用者一覧 (${index + 1})`)
      .setDescription(chunk),
  );

  await interaction.reply({
    embeds,
    flags: MessageFlags.Ephemeral,
  });
}

async function handleBorrowButton(interaction: ButtonInteraction): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  const accounts =
    latestRankedSnapshot?.sortedAccounts ?? (await sortAccountsByRank(await loadAccounts())).sortedAccounts;
  const state = await ensureState();

  if (findRentalByUser(state, interaction.user.id)) {
    await interaction.editReply({
      content: "すでにアカウントを借りています。先に返却してください。",
    });
    return;
  }

  await interaction.editReply({
    content: "借りるアカウントを選択してください。",
    components: [buildBorrowMenu(accounts, state, interaction.user.id)],
  });
}

async function handleReturnButton(interaction: ButtonInteraction): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  const accounts =
    latestRankedSnapshot?.sortedAccounts ?? (await sortAccountsByRank(await loadAccounts())).sortedAccounts;
  const currentState = await ensureState();
  const myRental = findRentalByUser(currentState, interaction.user.id);

  if (!myRental) {
    await interaction.editReply({
      content: "現在、返却できるアカウントはありません。",
    });
    return;
  }

  const account = accounts.find((item) => item.username === myRental.username);
  if (!account) {
    await interaction.editReply({
      content: "返却対象のアカウントが見つかりませんでした。",
    });
    return;
  }

  const returnTimestamp = new Date().toISOString();
  const returnResult = await updateState(async (state) => {
    const latestRental = findRentalByUser(state, interaction.user.id);
    if (!latestRental || latestRental.username !== myRental.username) {
      return { ok: false as const };
    }

    state.rentals = state.rentals.filter((record) => record.username !== myRental.username);
    addHistoryRecord(state, {
      action: "return",
      username: account.username,
      displayName: getDisplayName(account),
      userId: interaction.user.id,
      timestamp: returnTimestamp,
    });
    return { ok: true as const };
  });

  if (!returnResult.ok) {
    await interaction.editReply({
      content: "返却対象の状態が更新されました。もう一度パネルを確認してください。",
    });
    return;
  }

  await interaction.editReply({
    content: `\`${getDisplayName(account)}\` を返却しました。`,
  });

  runDetached(async () => {
    await refreshPanel();
  });
  runDetached(async () => {
    await sendLogMessage(`返却 | \`${getDisplayName(account)}\` | <@${interaction.user.id}> | ${formatTimestamp(returnTimestamp)}`);
  });
}

async function handleBorrowSelect(interaction: StringSelectMenuInteraction): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  const username = interaction.values[0];
  if (username === "none") {
    await interaction.editReply({
      content: "現在借りられるアカウントはありません。",
    });
    return;
  }

  const accounts = await loadAccounts();
  const account = accounts.find((item) => item.username === username);
  if (!account) {
    await interaction.editReply({
      content: "選択されたアカウントが見つかりませんでした。",
    });
    return;
  }

  const borrowTimestamp = new Date().toISOString();
  const borrowResult = await updateState(async (state) => {
    const existingRental = findRentalByUser(state, interaction.user.id);
    if (existingRental) {
      return { ok: false as const, reason: "already-borrowing" as const };
    }

    if (findRental(state, username)) {
      return { ok: false as const, reason: "already-rented" as const };
    }

    state.rentals.push({
      username,
      borrowedBy: interaction.user.id,
      borrowedAt: borrowTimestamp,
      reminderSentAt: undefined,
    });
    addHistoryRecord(state, {
      action: "borrow",
      username: account.username,
      displayName: getDisplayName(account),
      userId: interaction.user.id,
      timestamp: borrowTimestamp,
    });
    return { ok: true as const };
  });

  if (!borrowResult.ok) {
    await interaction.editReply({
      content:
        borrowResult.reason === "already-borrowing"
          ? "すでにアカウントを借りています。先に返却してください。"
          : "そのアカウントはすでに貸出中です。パネルを更新して確認してください。",
    });
    return;
  }

  const sideEffectErrors: string[] = [];
  try {
    await refreshPanel();
  } catch {
    sideEffectErrors.push("パネル更新");
  }
  try {
    await sendLogMessage(`貸出 | \`${getDisplayName(account)}\` | <@${interaction.user.id}> | ${formatTimestamp(borrowTimestamp)}`);
  } catch {
    sideEffectErrors.push("ログ送信");
  }

  const cachedRank = await getRankResult(account);
  const rankText = cachedRank.details ? formatRankText(cachedRank.details) : "Unrated";
  const lines = [
    "貸し出しを記録しました。",
    "",
    `アカウント: \`${getDisplayName(account)}\``,
    `ユーザー名: \`${account.username}\``,
    `パスワード: \`${account.password}\``,
  ];

  if (!isSameDisplayAndGameId(account)) {
    lines.push(`gameid: \`${account.gameId}\``);
  }

  lines.push(`ランク: ${rankText}`);
  if (sideEffectErrors.length > 0) {
    lines.push("");
    lines.push(`補足: ${sideEffectErrors.join("・")} に失敗しました。`);
  }

  await interaction.editReply({
    content: lines.join("\n"),
  });
}

client.once(Events.ClientReady, async (readyClient) => {
  const rest = new REST({ version: "10" }).setToken(TOKEN);
  await rest.put(Routes.applicationGuildCommands(CLIENT_ID, GUILD_ID), { body: commands });
  await ensureState();
  runDetached(checkBorrowReminders);
  setInterval(() => {
    runDetached(checkBorrowReminders);
  }, BORROW_REMINDER_CHECK_INTERVAL_MS);
  console.log(`${readyClient.user.tag} is ready.`);
});

client.on(Events.InteractionCreate, async (interaction: Interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      if (interaction.commandName === "panel") {
        await handlePanelCommand(interaction);
        return;
      }

      if (interaction.commandName === "account-add") {
        await handleAccountAddCommand(interaction);
        return;
      }

      if (interaction.commandName === "upload-emojis") {
        await handleUploadRankEmojisCommand(interaction);
        return;
      }

      if (interaction.commandName === "history") {
        await handleHistoryCommand(interaction);
        return;
      }
    }

    if (interaction.isButton()) {
      if (interaction.customId === "borrow") {
        await handleBorrowButton(interaction);
        return;
      }

      if (interaction.customId === "return") {
        await handleReturnButton(interaction);
        return;
      }

      if (interaction.customId === "refresh") {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await refreshPanelWithOptions({ forceRefresh: true });
        await interaction.editReply({ content: "パネルを更新しました。" });
        return;
      }
    }

    if (interaction.isStringSelectMenu()) {
      if (interaction.customId === "borrow-select") {
        await handleBorrowSelect(interaction);
        return;
      }
    }
  } catch (error) {
    console.error(error);

    if (error instanceof DiscordAPIError && error.code === 10062) {
      return;
    }

    const content =
      error instanceof Error ? `エラーが発生しました: ${error.message}` : "エラーが発生しました。";

    if (interaction.isRepliable()) {
      if (interaction.deferred || interaction.replied) {
        await interaction.followUp({ content, flags: MessageFlags.Ephemeral });
      } else {
        await interaction.reply({ content, flags: MessageFlags.Ephemeral });
      }
    }
  }
});

client.login(TOKEN);
