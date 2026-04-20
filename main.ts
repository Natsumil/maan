import "dotenv/config";
import {
  ActionRowBuilder,
  AttachmentBuilder,
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
  MessageCreateOptions,
  PermissionFlagsBits,
  REST,
  Routes,
  SlashCommandBuilder,
  StringSelectMenuBuilder,
  StringSelectMenuInteraction,
  User,
} from "discord.js";
import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import http from "node:http";
import path from "node:path";

type Account = {
  key: string;
  id: string;
  password: string;
  name: string;
  level: string;
  rank: string;
  unixTime: number | null;
};

type LoanRecord = {
  accountId: string;
  userId: string;
  borrowedAt: string;
};

type UserCreditState = {
  credits: number;
  lastRefreshedMonth: string;
};

type AppSettings = {
  monthlyCreditAmount: number;
};

type HistoryRecord = {
  action: string;
  timestamp: string;
  userId?: string;
  operatorUserId?: string;
  accountId?: string;
  accountName?: string;
  rank?: string;
  amount?: number;
  note?: string;
};

type AppState = {
  panelChannelId?: string;
  panelMessageId?: string;
  loans: LoanRecord[];
  users: Record<string, UserCreditState>;
  settings: AppSettings;
  history: HistoryRecord[];
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

function getEnvValuesWithPrefix(prefix: string): string[] {
  return Object.entries(process.env)
    .filter(
      ([key, value]) =>
        key === prefix || key.startsWith(`${prefix}_`) && typeof value === "string" && value.trim().length > 0,
    )
    .sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey, "en"))
    .map(([, value]) => value!.trim());
}

const TOKEN = requireEnv("DISCORD_TOKEN");
const GUILD_ID = requireEnv("DISCORD_GUILD_ID");
const LOG_CHANNEL_ID = process.env.LOG_CHANNEL_ID;
const HENRIK_API_KEYS = getEnvValuesWithPrefix("HENRIK_API_KEY");

const ACCOUNTS_FILE = path.resolve("accounts.txt");
const STATE_FILE = path.resolve("state.json");
const EMOJI_FILES_DIR = path.resolve("files");
const DEFAULT_MONTHLY_CREDIT_AMOUNT = 3;
const HISTORY_LIMIT = 300;
const EMOJI_CACHE_TTL_MS = 30 * 60 * 1000;
const JST_TIME_ZONE = "Asia/Tokyo";
const BORROW_DM_THUMBNAIL_URL =
  "https://media.discordapp.net/attachments/1495722642391699476/1495728158648045648/image.png?ex=69e74ce6&is=69e5fb66&hm=91101b0e84610031e9099005a6c8ccb11fcd38fad459aebcb323b278ec9866f3&=&format=webp&quality=lossless";
const KNOWN_RANKS = [
  "radiant",
  "immortal",
  "ascendant",
  "diamond",
  "platinum",
  "gold",
  "silver",
  "bronze",
  "iron",
  "unranked",
];

let stateOperationQueue: Promise<void> = Promise.resolve();
let emojiSnapshot: EmojiSnapshot | null = null;

const client = new Client({
  intents: [GatewayIntentBits.Guilds],
});

const adminOnly = PermissionFlagsBits.Administrator;

const commands = [
  new SlashCommandBuilder()
    .setName("panel")
    .setDescription("このチャンネルに在庫パネルを作成または更新します。")
    .setDefaultMemberPermissions(adminOnly),
  new SlashCommandBuilder()
    .setName("account")
    .setDescription("アカウントを追加または削除します。")
    .addSubcommand((subcommand) =>
      subcommand
        .setName("add")
        .setDescription("accounts.txt にアカウントを追加します。")
        .addStringOption((option) =>
          option
            .setName("entry")
            .setDescription("id:pass:name:level:rank:unixTime の1行")
            .setRequired(true),
        ),
    )
    .addSubcommand((subcommand) =>
      subcommand
        .setName("delete")
        .setDescription("accounts.txt からアカウントを削除します。")
        .addStringOption((option) =>
          option.setName("id").setDescription("削除するアカウントID").setRequired(true),
        ),
    )
    .addSubcommand((subcommand) =>
      subcommand
        .setName("send")
        .setDescription("現在の accounts.txt を実行者の DM に送信します。"),
    )
    .addSubcommand((subcommand) =>
      subcommand
        .setName("upload")
        .setDescription("アカウントリストのテキストファイルで accounts.txt を置き換えます。")
        .addAttachmentOption((option) =>
          option.setName("file").setDescription("アップロードする txt ファイル").setRequired(true),
        ),
    )
    .setDefaultMemberPermissions(adminOnly),
  new SlashCommandBuilder()
    .setName("upload-emojis")
    .setDescription("files フォルダから未登録の絵文字だけをこのGuildにアップロードします。")
    .setDefaultMemberPermissions(adminOnly),
  new SlashCommandBuilder()
    .setName("history")
    .setDescription("最近の貸出・クレジット操作履歴を表示します。")
    .setDefaultMemberPermissions(adminOnly),
  new SlashCommandBuilder()
    .setName("credit")
    .setDescription("クレジットを操作または確認します。")
    .addSubcommand((subcommand) =>
      subcommand
        .setName("add")
        .setDescription("指定ユーザーのクレジットを増やします。")
        .addUserOption((option) =>
          option.setName("user").setDescription("対象ユーザー").setRequired(true),
        )
        .addIntegerOption((option) =>
          option
            .setName("amount")
            .setDescription("増やす数")
            .setRequired(true)
            .setMinValue(1),
        ),
    )
    .addSubcommand((subcommand) =>
      subcommand
        .setName("remove")
        .setDescription("指定ユーザーのクレジットを減らします。")
        .addUserOption((option) =>
          option.setName("user").setDescription("対象ユーザー").setRequired(true),
        )
        .addIntegerOption((option) =>
          option
            .setName("amount")
            .setDescription("減らす数")
            .setRequired(true)
            .setMinValue(1),
        ),
    )
    .addSubcommand((subcommand) =>
      subcommand
        .setName("set")
        .setDescription("指定ユーザーの現在クレジットを上書きします。")
        .addUserOption((option) =>
          option.setName("user").setDescription("対象ユーザー").setRequired(true),
        )
        .addIntegerOption((option) =>
          option
            .setName("amount")
            .setDescription("設定する数")
            .setRequired(true)
            .setMinValue(0),
        ),
    )
    .addSubcommand((subcommand) =>
      subcommand
        .setName("show")
        .setDescription("指定ユーザーのクレジット状況を表示します。")
        .addUserOption((option) =>
          option.setName("user").setDescription("対象ユーザー").setRequired(true),
        ),
    )
    .setDefaultMemberPermissions(adminOnly),
  new SlashCommandBuilder()
    .setName("monthly-credit")
    .setDescription("月初クレジット配布設定を操作または確認します。")
    .addSubcommand((subcommand) =>
      subcommand
        .setName("set")
        .setDescription("来月以降の月初クレジット配布数を変更します。")
        .addIntegerOption((option) =>
          option
            .setName("amount")
            .setDescription("月初に配る数")
            .setRequired(true)
            .setMinValue(0),
        ),
    )
    .addSubcommand((subcommand) =>
      subcommand
        .setName("show")
        .setDescription("現在の月初クレジット配布設定を表示します。"),
    )
    .setDefaultMemberPermissions(adminOnly),
].map((command) => command.toJSON());

function normalizeInteger(value: number): number {
  return Math.max(0, Math.floor(value));
}

function createDefaultState(): AppState {
  return {
    loans: [],
    users: {},
    settings: {
      monthlyCreditAmount: DEFAULT_MONTHLY_CREDIT_AMOUNT,
    },
    history: [],
  };
}

function getCurrentMonthKey(): string {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: JST_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
  });
  const parts = formatter.formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value ?? "1970";
  const month = parts.find((part) => part.type === "month")?.value ?? "01";
  return `${year}-${month}`;
}

function getDefaultCredits(state: AppState): number {
  return normalizeInteger(state.settings.monthlyCreditAmount);
}

function getEffectiveUserCreditState(state: AppState, userId: string): UserCreditState {
  const monthKey = getCurrentMonthKey();
  const existing = state.users[userId];

  if (!existing || existing.lastRefreshedMonth !== monthKey) {
    const refreshed: UserCreditState = {
      credits: getDefaultCredits(state),
      lastRefreshedMonth: monthKey,
    };
    state.users[userId] = refreshed;
    return refreshed;
  }

  existing.credits = normalizeInteger(existing.credits);
  return existing;
}

function addHistoryRecord(state: AppState, record: HistoryRecord): void {
  state.history.unshift(record);
  state.history = state.history.slice(0, HISTORY_LIMIT);
}

async function ensureState(): Promise<AppState> {
  try {
    const raw = await fs.readFile(STATE_FILE, "utf8");
    const parsed = JSON.parse(raw) as Partial<AppState> & {
      rentals?: Array<{
        username?: string;
        borrowedBy?: string;
        borrowedAt?: string;
      }>;
    };

    const base = createDefaultState();
    const migratedLoans =
      Array.isArray(parsed.loans) && parsed.loans.length > 0
        ? parsed.loans
        : Array.isArray(parsed.rentals)
          ? parsed.rentals
              .filter(
                (rental) =>
                  typeof rental.username === "string" &&
                  rental.username.length > 0 &&
                  typeof rental.borrowedBy === "string" &&
                  rental.borrowedBy.length > 0 &&
                  typeof rental.borrowedAt === "string" &&
                  rental.borrowedAt.length > 0,
              )
              .map((rental) => ({
                accountId: rental.username as string,
                userId: rental.borrowedBy as string,
                borrowedAt: rental.borrowedAt as string,
              }))
          : [];

    const users: Record<string, UserCreditState> = {};
    if (parsed.users && typeof parsed.users === "object") {
      for (const [userId, value] of Object.entries(parsed.users)) {
        if (!value || typeof value !== "object") {
          continue;
        }

        const candidate = value as Partial<UserCreditState>;
        users[userId] = {
          credits: normalizeInteger(candidate.credits ?? base.settings.monthlyCreditAmount),
          lastRefreshedMonth:
            typeof candidate.lastRefreshedMonth === "string" && candidate.lastRefreshedMonth.length > 0
              ? candidate.lastRefreshedMonth
              : getCurrentMonthKey(),
        };
      }
    }

    return {
      panelChannelId: parsed.panelChannelId,
      panelMessageId: parsed.panelMessageId,
      loans: migratedLoans
        .filter(
          (loan) =>
            typeof loan.accountId === "string" &&
            loan.accountId.length > 0 &&
            typeof loan.userId === "string" &&
            loan.userId.length > 0 &&
            typeof loan.borrowedAt === "string" &&
            loan.borrowedAt.length > 0,
        )
        .map((loan) => ({
          accountId: loan.accountId,
          userId: loan.userId,
          borrowedAt: loan.borrowedAt,
        })),
      users,
      settings: {
        monthlyCreditAmount: normalizeInteger(
          parsed.settings?.monthlyCreditAmount ?? base.settings.monthlyCreditAmount,
        ),
      },
      history: Array.isArray(parsed.history)
        ? parsed.history
            .filter((entry) => entry && typeof entry === "object")
            .map((entry) => {
              const candidate = entry as Partial<HistoryRecord> & {
                username?: string;
                displayName?: string;
              };
              return {
                action: typeof candidate.action === "string" ? candidate.action : "legacy",
                timestamp:
                  typeof candidate.timestamp === "string"
                    ? candidate.timestamp
                    : new Date().toISOString(),
                userId: typeof candidate.userId === "string" ? candidate.userId : undefined,
                operatorUserId:
                  typeof candidate.operatorUserId === "string"
                    ? candidate.operatorUserId
                    : undefined,
                accountId:
                  typeof candidate.accountId === "string"
                    ? candidate.accountId
                    : typeof candidate.username === "string"
                      ? candidate.username
                      : undefined,
                accountName:
                  typeof candidate.accountName === "string"
                    ? candidate.accountName
                    : typeof candidate.displayName === "string"
                      ? candidate.displayName
                      : undefined,
                rank: typeof candidate.rank === "string" ? candidate.rank : undefined,
                amount:
                  typeof candidate.amount === "number" && Number.isFinite(candidate.amount)
                    ? candidate.amount
                    : undefined,
                note: typeof candidate.note === "string" ? candidate.note : undefined,
              };
            })
        : [],
    };
  } catch {
    const nextState = createDefaultState();
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

function ensureAccountValue(label: string, value: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0 || trimmed.includes(":") || /[\r\n]/.test(trimmed)) {
    throw new Error(`${label} は空欄不可で、改行とコロン ":" は使えません。`);
  }

  return trimmed;
}

function ensureOptionalAccountValue(label: string, value: string | null): string {
  if (value === null) {
    return "";
  }

  const trimmed = value.trim();
  if (trimmed.includes(":") || /[\r\n]/.test(trimmed)) {
    throw new Error(`${label} は改行とコロン ":" は使えません。`);
  }

  return trimmed;
}

function toAccountDisplayValue(value: string, fallback = "-"): string {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function getAccountTitle(account: Account): string {
  return toAccountDisplayValue(account.name, "Unknown Account");
}

function getAccountIdText(account: Account): string {
  return toAccountDisplayValue(account.id);
}

function getAccountPasswordText(account: Account): string {
  return toAccountDisplayValue(account.password);
}

function createStableAccountKey(parts: {
  id: string;
  password: string;
  name: string;
  level: string;
  rank: string;
  unixTime: number | null;
}): string {
  const normalizedId = parts.id.trim();
  if (normalizedId.length > 0) {
    return `id:${normalizedId}`;
  }

  const raw = [
    parts.id,
    parts.password,
    parts.name,
    parts.level,
    parts.rank,
    parts.unixTime === null ? "" : String(parts.unixTime),
  ].join("\u001f");
  const digest = createHash("sha256").update(raw).digest("hex").slice(0, 16);
  return `raw:${digest}`;
}

function parseAccountsText(raw: string, sourceLabel: string): Account[] {
  const accounts = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"))
    .map((line, index) => {
      const parts = line.split(":");
      if (parts.length < 4 || parts.length > 6) {
        throw new Error(
          `${sourceLabel} ${index + 1}行目の形式が不正です。id:pass:name:level:rank:unixtime を使ってください。`,
        );
      }

      const [id = "", password = "", name = "", ...rest] = parts;
      const unixTimeRaw = rest.pop() ?? "";
      const [level = "", rank = ""] = rest;
      const trimmedUnixTime = unixTimeRaw.trim();
      const unixTime =
        trimmedUnixTime.length === 0
          ? null
          : Number.isFinite(Number(trimmedUnixTime))
            ? Math.floor(Number(trimmedUnixTime))
            : Number.NaN;
      if (Number.isNaN(unixTime)) {
        throw new Error(`${sourceLabel} ${index + 1}行目の unixtime が不正です。`);
      }

      const accountParts = {
        id,
        password,
        name,
        level,
        rank,
        unixTime,
      };

      return {
        key: createStableAccountKey(accountParts),
        ...accountParts,
      };
    });

  const seenIds = new Set<string>();
  const seenKeys = new Set<string>();

  for (const account of accounts) {
    const normalizedId = account.id.trim();
    if (normalizedId.length > 0) {
      if (seenIds.has(normalizedId)) {
        throw new Error(`${sourceLabel} に重複した id があります: ${normalizedId}`);
      }
      seenIds.add(normalizedId);
    }

    if (seenKeys.has(account.key)) {
      throw new Error(`${sourceLabel} に重複したアカウント内容があります。`);
    }
    seenKeys.add(account.key);
  }

  return accounts;
}

async function loadAccounts(): Promise<Account[]> {
  const raw = await fs.readFile(ACCOUNTS_FILE, "utf8");
  return parseAccountsText(raw, "accounts.txt");
}

async function appendAccount(account: Account): Promise<void> {
  const unixTimeText = account.unixTime === null ? "" : String(account.unixTime);
  const line = `${account.id}:${account.password}:${account.name}:${account.level}:${account.rank}:${unixTimeText}\n`;
  await fs.appendFile(ACCOUNTS_FILE, line, "utf8");
}

async function replaceAccounts(accounts: Account[]): Promise<void> {
  const lines = accounts.map((account) => {
    const unixTimeText = account.unixTime === null ? "" : String(account.unixTime);
    return `${account.id}:${account.password}:${account.name}:${account.level}:${account.rank}:${unixTimeText}`;
  });

  const content = lines.length > 0 ? `${lines.join("\n")}\n` : "";
  await fs.writeFile(ACCOUNTS_FILE, content, "utf8");
}

async function removeAccount(accountId: string): Promise<boolean> {
  const raw = await fs.readFile(ACCOUNTS_FILE, "utf8");
  const lines = raw.split(/\r?\n/);
  let removed = false;

  const nextLines = lines.filter((line) => {
    const trimmed = line.trim();
    if (trimmed.length === 0 || trimmed.startsWith("#")) {
      return true;
    }

    const parts = trimmed.split(":");
    if (parts.length >= 4 && parts.length <= 6 && parts[0] === accountId) {
      removed = true;
      return false;
    }

    return true;
  });

  if (!removed) {
    return false;
  }

  let output = nextLines.join("\n");
  if (output.length > 0 && !output.endsWith("\n")) {
    output += "\n";
  }

  await fs.writeFile(ACCOUNTS_FILE, output, "utf8");
  return true;
}

type LogField = {
  name: string;
  value: string;
  inline?: boolean;
};

async function sendLogEmbed(options: {
  title: string;
  color?: number;
  fields: LogField[];
}): Promise<void> {
  if (!LOG_CHANNEL_ID) {
    return;
  }

  try {
    const channel = await client.channels.fetch(LOG_CHANNEL_ID);
    if (channel?.isTextBased() && "send" in channel) {
      await channel.send({
        embeds: [
          new EmbedBuilder()
            .setColor(options.color ?? 0x1f2937)
            .setTitle(options.title)
            .addFields(options.fields)
            .setTimestamp(),
        ],
      });
    }
  } catch (error) {
    console.error("Failed to send log embed:", error);
  }
}

async function sendDirectMessage(
  userId: string,
  payload: string | MessageCreateOptions,
): Promise<boolean> {
  try {
    const user = await client.users.fetch(userId);
    await user.send(payload);
    return true;
  } catch (error) {
    console.error("Failed to send direct message:", error);
    return false;
  }
}

async function handleAccountSendCommand(interaction: ChatInputCommandInteraction): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  try {
    const content = await fs.readFile(ACCOUNTS_FILE, "utf8");
    const attachment = new AttachmentBuilder(Buffer.from(content, "utf8"), {
      name: "accounts.txt",
    });

    const sent = await sendDirectMessage(interaction.user.id, {
      content: "現在の accounts.txt です。",
      files: [attachment],
    });

    if (!sent) {
      await interaction.editReply({
        content: "DM を送れませんでした。DM を開放してからもう一度試してください。",
      });
      return;
    }

    await interaction.editReply({
      content: "accounts.txt を DM に送信しました。",
    });
  } catch (error) {
    await interaction.editReply({
      content:
        error instanceof Error
          ? `accounts.txt の送信に失敗しました: ${error.message}`
          : "accounts.txt の送信に失敗しました。",
    });
  }
}

async function handleAccountUploadCommand(interaction: ChatInputCommandInteraction): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  try {
    const attachment = interaction.options.getAttachment("file", true);
    const response = await fetch(attachment.url);
    if (!response.ok) {
      throw new Error(`ファイル取得に失敗しました: ${response.status} ${response.statusText}`);
    }

    const content = await response.text();
    const uploadedAccounts = parseAccountsText(content, attachment.name ?? "uploaded file");
    await replaceAccounts(uploadedAccounts);
    await updateState(async (state) => {
      state.loans = [];
    });
    await refreshPanel();

    await interaction.editReply({
      content: [
        `accounts.txt を置き換えました。`,
        `反映: ${uploadedAccounts.length}件`,
        `貸出状態: クリア済み`,
      ].join("\n"),
    });

    runDetached(async () => {
      await sendLogEmbed({
        title: "アカウント一括置換",
        color: 0x059669,
        fields: [
          { name: "File", value: `\`${attachment.name ?? "upload.txt"}\``, inline: true },
          { name: "Replaced", value: `\`${uploadedAccounts.length}\``, inline: true },
          { name: "Loans", value: "`cleared`", inline: true },
          { name: "Operator", value: `<@${interaction.user.id}>`, inline: true },
        ],
      });
    });
  } catch (error) {
    await interaction.editReply({
      content:
        error instanceof Error
          ? `アカウント置換に失敗しました: ${error.message}`
          : "アカウント置換に失敗しました。",
    });
  }
}

function runDetached(task: () => Promise<void>): void {
  void task().catch((error) => {
    console.error("Detached task failed:", error);
  });
}

function normalizeEmojiName(value: string): string {
  const normalized = value.toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
  if (normalized.length >= 2) {
    return normalized.slice(0, 32);
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
    const emojiName = normalizeEmojiName(path.parse(fileName).name);
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

function resolveRankEmoji(rankKey: string, emojis: Map<string, string>): string {
  const directCandidates = [
    normalizeEmojiName(rankKey),
    rankKey.toLowerCase(),
  ];

  for (const candidate of directCandidates) {
    const direct = emojis.get(candidate);
    if (direct) {
      return direct;
    }
  }

  for (const [emojiName, mention] of emojis.entries()) {
    const normalizedEmojiName = normalizeEmojiName(emojiName);
    if (
      normalizedEmojiName === normalizeEmojiName(rankKey) ||
      normalizedEmojiName.startsWith(`${normalizeEmojiName(rankKey)}_`)
    ) {
      return mention;
    }
  }

  return "";
}

function formatTimestamp(isoDate: string): string {
  const unixSeconds = Math.floor(new Date(isoDate).getTime() / 1000);
  return `<t:${unixSeconds}:f>`;
}

function formatUnixTimestamp(unixTime: number | null): string {
  if (unixTime === null) {
    return "-";
  }
  return `<t:${Math.floor(unixTime)}:f>`;
}

function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength - 1)}…`;
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

function buildChunkedFields(
  label: string,
  lines: string[],
  maxLength: number,
): Array<{ name: string; value: string; inline?: boolean }> {
  return chunkLines(lines, maxLength).map((chunk, index) => ({
    name: index === 0 ? label : `${label} (${index + 1})`,
    value: chunk,
  }));
}

function findLoan(state: AppState, accountId: string): LoanRecord | undefined {
  return state.loans.find((loan) => loan.accountId === accountId);
}

function getLoanedAccountIds(state: AppState): Set<string> {
  return new Set(state.loans.map((loan) => loan.accountId));
}

type RankPresentation = {
  key: string;
  label: string;
};

function getRankColor(rankKey: string): number {
  const colors: Record<string, number> = {
    radiant: 0xf4b942,
    immortal: 0xb24cff,
    ascendant: 0x3dc98f,
    diamond: 0x6aa9ff,
    platinum: 0x49c5b6,
    gold: 0xe0b24a,
    silver: 0xaab4c2,
    bronze: 0xb97a56,
    iron: 0x7f8793,
    unranked: 0x4b5563,
  };

  return colors[rankKey] ?? 0x111827;
}

function titleCaseRankText(value: string): string {
  return value
    .split(/\s+/)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function resolveNumericRankPresentation(rankValue: number): RankPresentation {
  if (rankValue <= 2) {
    return { key: "unranked", label: "Unranked" };
  }

  const table: Array<{ max: number; key: string; start: number; name: string }> = [
    { max: 5, key: "iron", start: 3, name: "Iron" },
    { max: 8, key: "bronze", start: 6, name: "Bronze" },
    { max: 11, key: "silver", start: 9, name: "Silver" },
    { max: 14, key: "gold", start: 12, name: "Gold" },
    { max: 17, key: "platinum", start: 15, name: "Platinum" },
    { max: 20, key: "diamond", start: 18, name: "Diamond" },
    { max: 23, key: "ascendant", start: 21, name: "Ascendant" },
    { max: 26, key: "immortal", start: 24, name: "Immortal" },
  ];

  for (const entry of table) {
    if (rankValue <= entry.max) {
      return {
        key: entry.key,
        label: `${entry.name} ${rankValue - entry.start + 1}`,
      };
    }
  }

  return { key: "radiant", label: "Radiant" };
}

function resolveNumericRankKey(rankValue: number): string {
  return resolveNumericRankPresentation(rankValue).key;
}

function resolveRankPresentation(rank: string): RankPresentation {
  const normalized = rank.trim().toLowerCase();
  if (normalized.length === 0) {
    return { key: "unranked", label: "Unranked" };
  }

  if (/^\d+$/.test(normalized)) {
    return resolveNumericRankPresentation(Number(normalized));
  }

  for (const knownRank of KNOWN_RANKS) {
    if (normalized === knownRank) {
      return {
        key: knownRank,
        label: titleCaseRankText(knownRank),
      };
    }

    if (normalized.startsWith(`${knownRank} `)) {
      return {
        key: knownRank,
        label: titleCaseRankText(normalized),
      };
    }
  }

  return {
    key: normalized,
    label: titleCaseRankText(normalized),
  };
}

function resolveRankKey(rank: string): string {
  return resolveRankPresentation(rank).key;
}

function normalizeRankKey(rank: string): string {
  return resolveRankKey(rank);
}

function formatRankLabel(rank: string): string {
  const trimmed = resolveRankKey(rank);
  if (trimmed.length === 0) {
    return "不明";
  }

  return titleCaseRankText(trimmed);
}

function formatInventoryCount(value: number, width: number): string {
  return String(value).padStart(width, " ");
}

function getRankGroups(accounts: Account[], state: AppState) {
  const loaned = getLoanedAccountIds(state);
  const groups = new Map<
    string,
    {
      label: string;
      availableAccounts: Account[];
      totalAccounts: number;
    }
  >();

  for (const standardRank of KNOWN_RANKS) {
    groups.set(standardRank, {
      label: formatRankLabel(standardRank),
      availableAccounts: [],
      totalAccounts: 0,
    });
  }

  for (const account of accounts) {
    const key = normalizeRankKey(account.rank);
    if (!groups.has(key)) {
      groups.set(key, {
        label: formatRankLabel(account.rank),
        availableAccounts: [],
        totalAccounts: 0,
      });
    }

    const group = groups.get(key)!;
    group.totalAccounts += 1;
    if (!loaned.has(account.key)) {
      group.availableAccounts.push(account);
    }
  }

  return [...groups.entries()].sort((left, right) => {
    const leftIndex = KNOWN_RANKS.indexOf(left[0]);
    const rightIndex = KNOWN_RANKS.indexOf(right[0]);

    if (leftIndex !== -1 || rightIndex !== -1) {
      if (leftIndex === -1) {
        return 1;
      }
      if (rightIndex === -1) {
        return -1;
      }
      return leftIndex - rightIndex;
    }

    return left[1].label.localeCompare(right[1].label, "ja");
  });
}

async function buildPanelEmbed(
  accounts: Account[],
  state: AppState,
  options?: { forceRefresh?: boolean },
): Promise<EmbedBuilder> {
  const emojis = await getEmojiMentionMap(options?.forceRefresh ?? false);
  const groups = getRankGroups(accounts, state);
  const availableCount = groups.reduce(
    (sum, [, group]) => sum + group.availableAccounts.length,
    0,
  );
  const totalCount = groups.reduce((sum, [, group]) => sum + group.totalAccounts, 0);
  const labelWidth = Math.max(...groups.map(([, group]) => group.label.length));
  const countWidth = Math.max(
    2,
    ...groups.flatMap(([, group]) => [
      String(group.availableAccounts.length).length,
      String(group.totalAccounts).length,
    ]),
  );

  const lines = groups.map(([rankKey, group]) => {
    const emoji = resolveRankEmoji(rankKey, emojis);
    const label = group.label.padEnd(labelWidth, " ");
    const available = formatInventoryCount(group.availableAccounts.length, countWidth);
    const total = formatInventoryCount(group.totalAccounts, countWidth);
    const prefix = emoji ? `${emoji} ` : "";
    return `${prefix}\`${label}  ${available} / ${total}\``;
  });

  return new EmbedBuilder()
    .setColor(0x2b2d31)
    .setTitle("アカウント在庫")
    .setDescription(`利用可能 ${availableCount} / 全体 ${totalCount}`)
    .addFields(...buildChunkedFields("\u200b", lines, 1024));
}

function buildPanelComponents() {
  return [
    new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder()
        .setCustomId("borrow")
        .setLabel("借りる")
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId("refresh")
        .setLabel("更新")
        .setStyle(ButtonStyle.Secondary),
    ),
  ];
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

async function refreshPanel(): Promise<void> {
  await refreshPanelWithOptions();
}

function buildBorrowMenu(accounts: Account[], state: AppState, userCredits: number) {
  const groups = getRankGroups(accounts, state).filter(([, group]) => group.availableAccounts.length > 0);

  return new ActionRowBuilder<StringSelectMenuBuilder>().addComponents(
    new StringSelectMenuBuilder()
      .setCustomId("borrow-rank")
      .setPlaceholder(
        userCredits > 0
          ? groups.length > 0
            ? "借りるランクを選択"
            : "利用可能な在庫がありません"
          : "クレジットがありません",
      )
      .setDisabled(userCredits <= 0 || groups.length === 0)
      .addOptions(
        (groups.length > 0
          ? groups
          : [["none", { label: "在庫なし", availableAccounts: [], totalAccounts: 0 }] as const]
        ).map(([rankKey, group]) => ({
          label: truncate(group.label, 100),
          description:
            rankKey === "none"
              ? " "
              : `在庫 ${group.availableAccounts.length} | 消費 1 クレジット`,
          value: rankKey,
        })),
      ),
  );
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
        // recreate below
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
  try {
    const entry = interaction.options.getString("entry", true).trim();
    const [account] = parseAccountsText(entry, "/account add");

    const accounts = await loadAccounts();
    if (account.id.trim().length > 0 && accounts.some((item) => item.id === account.id)) {
      await interaction.reply({
        content: `\`${account.id}\` はすでに登録されています。`,
        flags: MessageFlags.Ephemeral,
      });
      return;
    }

    await appendAccount(account);
    await refreshPanel();

    await interaction.reply({
      content: `\`${getAccountTitle(account)}\` を accounts.txt に追加しました。`,
      flags: MessageFlags.Ephemeral,
    });
  } catch (error) {
    await interaction.reply({
      content: error instanceof Error ? error.message : "アカウント追加に失敗しました。",
      flags: MessageFlags.Ephemeral,
    });
  }
}

async function handleAccountDeleteCommand(interaction: ChatInputCommandInteraction): Promise<void> {
  const accountIdRaw = interaction.options.getString("id", true);
  let accountId: string;

  try {
    accountId = ensureAccountValue("id", accountIdRaw);
  } catch (error) {
    await interaction.reply({
      content: error instanceof Error ? error.message : "id が不正です。",
      flags: MessageFlags.Ephemeral,
    });
    return;
  }

  const accounts = await loadAccounts();
  const account = accounts.find((item) => item.id === accountId);
  if (!account) {
    await interaction.reply({
      content: `\`${accountId}\` は登録されていません。`,
      flags: MessageFlags.Ephemeral,
    });
    return;
  }

  const removed = await removeAccount(accountId);
  if (!removed) {
    await interaction.reply({
      content: `\`${accountId}\` の削除に失敗しました。もう一度お試しください。`,
      flags: MessageFlags.Ephemeral,
    });
    return;
  }

  const stateResult = await updateState(async (state) => {
    const removedLoan = findLoan(state, account.key);
    state.loans = state.loans.filter((loan) => loan.accountId !== account.key);
    return { removedLoan };
  });

  await refreshPanel();

  const lines = [`\`${account.name}\` を accounts.txt から削除しました。`];
  if (stateResult.removedLoan) {
    lines.push(`貸出記録も解除しました。対象ユーザー: <@${stateResult.removedLoan.userId}>`);
  }

  await interaction.reply({
    content: lines.join("\n"),
    flags: MessageFlags.Ephemeral,
  });

  runDetached(async () => {
    await sendLogEmbed({
      title: "アカウント削除",
      color: 0xb91c1c,
      fields: [
        { name: "Account", value: `\`${getAccountTitle(account)}\``, inline: true },
        { name: "ID", value: `\`${getAccountIdText(account)}\``, inline: true },
        { name: "Operator", value: `<@${interaction.user.id}>`, inline: true },
        { name: "Rank", value: `\`${resolveRankPresentation(account.rank).label}\``, inline: true },
      ],
    });
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

function formatHistoryRecord(entry: HistoryRecord): string {
  const at = formatTimestamp(entry.timestamp);

  if (entry.action === "borrow") {
    return `${at} | 貸出 | <@${entry.userId}> | \`${entry.accountName ?? entry.accountId ?? "unknown"}\` | ${entry.rank ?? "不明"} | ${entry.note ?? ""}`.trim();
  }

  if (entry.action === "credit_add") {
    return `${at} | credit-add | <@${entry.userId}> | +${entry.amount ?? 0} | 実行者 <@${entry.operatorUserId}>`;
  }

  if (entry.action === "credit_remove") {
    return `${at} | credit-remove | <@${entry.userId}> | -${entry.amount ?? 0} | 実行者 <@${entry.operatorUserId}>`;
  }

  if (entry.action === "credit_set") {
    return `${at} | credit-set | <@${entry.userId}> | ${entry.amount ?? 0} に設定 | 実行者 <@${entry.operatorUserId}>`;
  }

  if (entry.action === "monthly_credit_set") {
    return `${at} | monthly-credit-set | ${entry.amount ?? 0} | 実行者 <@${entry.operatorUserId}>`;
  }

  return `${at} | ${entry.action} | ${entry.note ?? ""}`.trim();
}

async function handleHistoryCommand(interaction: ChatInputCommandInteraction): Promise<void> {
  const state = await ensureState();
  const lines = state.history.map(formatHistoryRecord);
  const chunks = chunkLines(lines.length > 0 ? lines : ["履歴はありません。"], 3800);
  const embeds = chunks.slice(0, 10).map((chunk, index) =>
    new EmbedBuilder()
      .setColor(0x2b2d31)
      .setTitle(index === 0 ? "履歴" : `履歴 (${index + 1})`)
      .setDescription(chunk),
  );

  await interaction.reply({
    embeds,
    flags: MessageFlags.Ephemeral,
  });
}

async function handleCreditMutationCommand(
  interaction: ChatInputCommandInteraction,
  mode: "add" | "remove" | "set",
): Promise<void> {
  const targetUser = interaction.options.getUser("user", true);
  const amount = normalizeInteger(interaction.options.getInteger("amount", true));

  const result = await updateState(async (state) => {
    const userState = getEffectiveUserCreditState(state, targetUser.id);

    if (mode === "add") {
      userState.credits += amount;
      addHistoryRecord(state, {
        action: "credit_add",
        timestamp: new Date().toISOString(),
        userId: targetUser.id,
        operatorUserId: interaction.user.id,
        amount,
      });
    } else if (mode === "remove") {
      userState.credits = Math.max(0, userState.credits - amount);
      addHistoryRecord(state, {
        action: "credit_remove",
        timestamp: new Date().toISOString(),
        userId: targetUser.id,
        operatorUserId: interaction.user.id,
        amount,
      });
    } else {
      userState.credits = amount;
      addHistoryRecord(state, {
        action: "credit_set",
        timestamp: new Date().toISOString(),
        userId: targetUser.id,
        operatorUserId: interaction.user.id,
        amount,
      });
    }

    return {
      credits: userState.credits,
      monthKey: userState.lastRefreshedMonth,
    };
  });

  const labels = {
    add: "増やしました",
    remove: "減らしました",
    set: "設定しました",
  } as const;

  await interaction.reply({
    content: `<@${targetUser.id}> のクレジットを${labels[mode]}。\n現在: ${result.credits}\n適用月: ${result.monthKey}`,
    flags: MessageFlags.Ephemeral,
  });

  runDetached(async () => {
    const title =
      mode === "add" ? "クレジット加算" : mode === "remove" ? "クレジット減算" : "クレジット設定";
    const delta = mode === "add" ? `+${amount}` : mode === "remove" ? `-${amount}` : `${amount}`;
    await sendLogEmbed({
      title,
      color: mode === "remove" ? 0xb45309 : 0x2563eb,
      fields: [
        { name: "User", value: `<@${targetUser.id}>`, inline: true },
        { name: "Amount", value: `\`${delta}\``, inline: true },
        { name: "Current", value: `\`${result.credits}\``, inline: true },
        { name: "Operator", value: `<@${interaction.user.id}>`, inline: true },
      ],
    });
  });
}

async function handleCreditShowCommand(interaction: ChatInputCommandInteraction): Promise<void> {
  const targetUser = interaction.options.getUser("user", true);

  const result = await updateState(async (state) => {
    const userState = getEffectiveUserCreditState(state, targetUser.id);
    return {
      credits: userState.credits,
      monthKey: userState.lastRefreshedMonth,
      monthlyCreditAmount: getDefaultCredits(state),
    };
  });

  await interaction.reply({
    content: [
      `対象: <@${targetUser.id}>`,
      `現在クレジット: ${result.credits}`,
      `今月判定: ${result.monthKey}`,
      `月初配布設定: ${result.monthlyCreditAmount}`,
    ].join("\n"),
    flags: MessageFlags.Ephemeral,
  });
}

async function handleMonthlyCreditSetCommand(
  interaction: ChatInputCommandInteraction,
): Promise<void> {
  const amount = normalizeInteger(interaction.options.getInteger("amount", true));

  await updateState(async (state) => {
    state.settings.monthlyCreditAmount = amount;
    addHistoryRecord(state, {
      action: "monthly_credit_set",
      timestamp: new Date().toISOString(),
      operatorUserId: interaction.user.id,
      amount,
    });
  });

  await interaction.reply({
    content: `月初配布クレジット数を ${amount} に変更しました。\n来月以降の月次更新から反映されます。`,
    flags: MessageFlags.Ephemeral,
  });

  runDetached(async () => {
    await sendLogEmbed({
      title: "月初クレジット設定変更",
      color: 0x4f46e5,
      fields: [
        { name: "Monthly Credit", value: `\`${amount}\``, inline: true },
        { name: "Operator", value: `<@${interaction.user.id}>`, inline: true },
      ],
    });
  });
}

async function handleMonthlyCreditShowCommand(
  interaction: ChatInputCommandInteraction,
): Promise<void> {
  const state = await ensureState();
  await interaction.reply({
    content: [
      `月初配布クレジット数: ${getDefaultCredits(state)}`,
      `現在月キー(JST): ${getCurrentMonthKey()}`,
    ].join("\n"),
    flags: MessageFlags.Ephemeral,
  });
}

async function handleBorrowButton(interaction: ButtonInteraction): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  const accounts = await loadAccounts();
  const state = await updateState(async (draft) => {
    const userState = getEffectiveUserCreditState(draft, interaction.user.id);
    return {
      snapshot: draft,
      credits: userState.credits,
    };
  });

  await interaction.editReply({
    content: `現在のクレジット: ${state.credits}\n借りるランクを選択してください。`,
    components: [buildBorrowMenu(accounts, state.snapshot, state.credits)],
  });
}

function buildBorrowDmEmbed(account: Account, user: User, remainingCredits: number): EmbedBuilder {
  const rankPresentation = resolveRankPresentation(account.rank);
  const accentColor = getRankColor(rankPresentation.key);
  const levelText = account.level.trim().length > 0 ? account.level : "-";
  return new EmbedBuilder()
    .setColor(accentColor)
    .setTitle(getAccountTitle(account))
    .setThumbnail(BORROW_DM_THUMBNAIL_URL)
    .addFields(
      {
        name: "ID",
        value: `\`${getAccountIdText(account)}\``,
        inline: true,
      },
      {
        name: "PASS",
        value: `\`${getAccountPasswordText(account)}\``,
        inline: true,
      },
      {
        name: "\u200b",
        value: "\u200b",
        inline: true,
      },
      {
        name: "Rank",
        value: `\`${rankPresentation.label}\``,
        inline: true,
      },
      {
        name: "Level",
        value: `\`${levelText}\``,
        inline: true,
      },
      {
        name: "Last Active",
        value: formatUnixTimestamp(account.unixTime),
        inline: true,
      },
    )
    .setTimestamp();
}

async function handleBorrowSelect(interaction: StringSelectMenuInteraction): Promise<void> {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });

  const selectedRank = interaction.values[0];
  if (selectedRank === "none") {
    await interaction.editReply({
      content: "現在借りられる在庫がありません。",
    });
    return;
  }

  const accounts = await loadAccounts();
  const borrowTimestamp = new Date().toISOString();

  const borrowResult = await updateState(async (state) => {
    const userState = getEffectiveUserCreditState(state, interaction.user.id);
    if (userState.credits <= 0) {
      return { ok: false as const, reason: "no-credit" as const };
    }

    const loaned = getLoanedAccountIds(state);
    const account = accounts.find(
      (item) => normalizeRankKey(item.rank) === selectedRank && !loaned.has(item.key),
    );

    if (!account) {
      return { ok: false as const, reason: "no-stock" as const };
    }

    userState.credits -= 1;
    state.loans.push({
      accountId: account.key,
      userId: interaction.user.id,
      borrowedAt: borrowTimestamp,
    });
    addHistoryRecord(state, {
      action: "borrow",
      timestamp: borrowTimestamp,
      userId: interaction.user.id,
      accountId: account.key,
      accountName: getAccountTitle(account),
      rank: account.rank,
      note: `残りクレジット ${userState.credits}`,
    });

    return {
      ok: true as const,
      account,
      remainingCredits: userState.credits,
    };
  });

  if (!borrowResult.ok) {
    await interaction.editReply({
      content:
        borrowResult.reason === "no-credit"
          ? "クレジットがありません。"
          : "そのランクの在庫はもうありません。パネルを更新して確認してください。",
    });
    return;
  }

  const dmSent = await sendDirectMessage(
    interaction.user.id,
    {
      embeds: [
        buildBorrowDmEmbed(
          borrowResult.account,
          interaction.user,
          borrowResult.remainingCredits,
        ),
      ],
    },
  );

  if (!dmSent) {
    await updateState(async (state) => {
      const userState = getEffectiveUserCreditState(state, interaction.user.id);
      userState.credits += 1;
      state.loans = state.loans.filter(
        (loan) =>
          !(
            loan.accountId === borrowResult.account.key &&
            loan.userId === interaction.user.id &&
            loan.borrowedAt === borrowTimestamp
          ),
      );
      state.history = state.history.filter(
        (entry) =>
          !(
            entry.action === "borrow" &&
            entry.accountId === borrowResult.account.key &&
            entry.userId === interaction.user.id &&
            entry.timestamp === borrowTimestamp
          ),
      );
    });

    await interaction.editReply({
      content: "DM を送れなかったため、貸出を取り消しました。DM を開放してからもう一度試してください。",
    });
    return;
  }

  await interaction.editReply({
    content: [
      `\`${getAccountTitle(borrowResult.account)}\` を貸し出しました。DM を確認してください。`,
      `残りクレジット: ${borrowResult.remainingCredits}`,
    ].join("\n"),
  });

  runDetached(async () => {
    await refreshPanel();
  });
  runDetached(async () => {
    await sendLogEmbed({
      title: "アカウント貸出",
      color: getRankColor(resolveRankPresentation(borrowResult.account.rank).key),
      fields: [
        { name: "User", value: `<@${interaction.user.id}>`, inline: true },
        { name: "Account", value: `\`${getAccountTitle(borrowResult.account)}\``, inline: true },
        {
          name: "Rank",
          value: `\`${resolveRankPresentation(borrowResult.account.rank).label}\``,
          inline: true,
        },
        { name: "Credits Left", value: `\`${borrowResult.remainingCredits}\``, inline: true },
      ],
    });
  });
}

client.once(Events.ClientReady, async (readyClient) => {
  const applicationId = readyClient.application.id;
  const rest = new REST({ version: "10" }).setToken(TOKEN);
  try {
    await rest.put(Routes.applicationGuildCommands(applicationId, GUILD_ID), { body: commands });
  } catch (error) {
    if (error instanceof DiscordAPIError) {
      console.error("Failed to register guild commands.");
      console.error(`Application ID: ${applicationId}`);
      console.error(`Guild ID: ${GUILD_ID}`);
      console.error(`Discord API error ${error.code}: ${error.message}`);

      if (error.code === 50001) {
        console.error(
          "The bot/application likely does not have access to this guild. Check that the bot is invited to the target server and that DISCORD_GUILD_ID is correct.",
        );
      }

      if (error.code === 20012) {
        console.error(
          "The token does not belong to the application you are trying to modify. Check that the bot token matches the intended Discord application.",
        );
      }
    }

    throw error;
  }
  await ensureState();
  console.log(`Henrik API keys loaded: ${HENRIK_API_KEYS.length}`);
  console.log(`Application ID: ${applicationId}`);
  console.log(`${readyClient.user.tag} is ready.`);
});

client.on(Events.InteractionCreate, async (interaction: Interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      if (interaction.commandName === "panel") {
        await handlePanelCommand(interaction);
        return;
      }

      if (interaction.commandName === "account") {
        const subcommand = interaction.options.getSubcommand();
        if (subcommand === "add") {
          await handleAccountAddCommand(interaction);
          return;
        }

        if (subcommand === "delete") {
          await handleAccountDeleteCommand(interaction);
          return;
        }

        if (subcommand === "send") {
          await handleAccountSendCommand(interaction);
          return;
        }

        if (subcommand === "upload") {
          await handleAccountUploadCommand(interaction);
          return;
        }

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

      if (interaction.commandName === "credit") {
        const subcommand = interaction.options.getSubcommand();
        if (subcommand === "show") {
          await handleCreditShowCommand(interaction);
          return;
        }

        if (subcommand === "add" || subcommand === "remove" || subcommand === "set") {
          await handleCreditMutationCommand(interaction, subcommand);
          return;
        }

        return;
      }

      if (interaction.commandName === "monthly-credit") {
        const subcommand = interaction.options.getSubcommand();
        if (subcommand === "set") {
          await handleMonthlyCreditSetCommand(interaction);
          return;
        }

        if (subcommand === "show") {
          await handleMonthlyCreditShowCommand(interaction);
          return;
        }

        return;
      }
    }

    if (interaction.isButton()) {
      if (interaction.customId === "borrow") {
        await handleBorrowButton(interaction);
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
      if (interaction.customId === "borrow-rank") {
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

http
  .createServer((_, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Azure Health Check OK");
  })
  .listen(process.env.PORT || 8080);

client.login(TOKEN);
