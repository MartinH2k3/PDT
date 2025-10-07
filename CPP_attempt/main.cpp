#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "json.hpp"

using json = nlohmann::json;
namespace fs = std::filesystem;


// ------------------------------ Config ------------------------------
static inline int env_int(const char* key, int fallback) {
    if (const char* v = std::getenv(key)) {
        try { return std::stoi(v); } catch (...) {}
    }
    return fallback;
}
const int BATCH_SIZE   = env_int("BATCH_SIZE",   10000);
const int RETRY_LIMIT  = env_int("RETRY_LIMIT",  3);     // kept for parity; not used specially
const int WORKER_COUNT = env_int("WORKER_COUNT", 16);

// ------------------------------ Logger ------------------------------
struct Logger {
    std::mutex m;
    std::ofstream out;
    explicit Logger(const std::string& path) : out(path, std::ios::app) {}
    template<class... Args>
    void info(Args&&... args) { line("INFO", std::forward<Args>(args)...); }
    template<class... Args>
    void error(Args&&... args) { line("ERROR", std::forward<Args>(args)...); }
    template<class... Args>
    void line(const char* level, Args&&... args) {
        std::lock_guard<std::mutex> lk(m);
        out << timestamp() << " [" << level << "] ";
        (out << ... << args) << "\n";
        out.flush();
    }
    static std::string timestamp() {
        using namespace std::chrono;
        auto now = system_clock::now();
        std::time_t t = system_clock::to_time_t(now);
        std::tm tm{};
        #ifdef _WIN32
        localtime_s(&tm, &t);
        #else
        localtime_r(&t, &tm);
        #endif
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }
};

Logger LOG("csv_log.txt");

// ------------------------------ CSV helpers ------------------------------
static inline std::string csv_quote(const std::string& s) {
    bool need = s.find_first_of(",\"\n\r") != std::string::npos;
    if (!need) return s;
    std::string out; out.reserve(s.size() + 2);
    out.push_back('"');
    for (char c : s) { if (c == '"') out.push_back('"'); out.push_back(c); }
    out.push_back('"');
    return out;
}

static inline void csv_write_rows(const std::string& path,
                                  const std::vector<std::vector<std::string>>& rows)
{
    if (rows.empty()) return;
    fs::create_directories(fs::path(path).parent_path());
    std::ofstream f(path, std::ios::app | std::ios::binary);
    for (const auto& r : rows) {
        for (size_t i = 0; i < r.size(); ++i) {
            if (i) f << ',';
            f << csv_quote(r[i]);
        }
        f << "\n";
    }
}

// ------------------------------ Hashing for tuples/pairs ------------------------------
struct PairHashI64I64 {
    size_t operator()(const std::pair<long long, long long>& p) const noexcept {
        auto h1 = std::hash<long long>{}(p.first);
        auto h2 = std::hash<long long>{}(p.second);
        // simple combine
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1<<6) + (h1>>2));
    }
};
struct PairHashI64Str {
    size_t operator()(const std::pair<long long, std::string>& p) const noexcept {
        auto h1 = std::hash<long long>{}(p.first);
        auto h2 = std::hash<std::string>{}(p.second);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1<<6) + (h1>>2));
    }
};

// ------------------------------ Globals (dedupe sets) ------------------------------
std::unordered_set<long long> users_set;
std::shared_mutex users_mtx;

std::unordered_set<std::string> places_set;
std::shared_mutex places_mtx;

std::unordered_set<long long> tweets_set;
std::shared_mutex tweets_mtx;

std::unordered_map<std::string, int> hashtags_map;
int curr_hashtag_id = 1;
std::mutex hashtags_mtx;

std::unordered_set<std::pair<long long,int>, PairHashI64I64> tweet_hashtags_set;
std::shared_mutex tweet_hashtags_mtx;

std::unordered_set<std::pair<long long,std::string>, PairHashI64Str> urls_set;
std::shared_mutex urls_mtx;

std::unordered_set<std::pair<long long,long long>, PairHashI64I64> media_set;
std::shared_mutex media_mtx;

std::unordered_set<std::pair<long long,long long>, PairHashI64I64> user_mentions_set;
std::shared_mutex user_mentions_mtx;

std::unordered_set<long long> missing_mentioned_users_set;
std::shared_mutex missing_users_mtx;

std::atomic<double> io_time_sec{0.0};

// ------------------------------ Helpers to read JSON safely ------------------------------
static inline std::string sget(const json& j, const char* key) {
    if (!j.contains(key) || j[key].is_null()) return "";
    if (j[key].is_string()) return j[key].get<std::string>();
    return j[key].dump(); // fallback
}
static inline long long iget_ll(const json& j, const char* key) {
    if (!j.contains(key) || j[key].is_null()) return 0;
    if (j[key].is_number_integer() || j[key].is_number_unsigned()) return j[key].get<long long>();
    try { return std::stoll(j[key].get<std::string>()); } catch(...) { return 0; }
}
static inline bool bget(const json& j, const char* key, bool def=false) {
    if (!j.contains(key) || j[key].is_null()) return def;
    if (j[key].is_boolean()) return j[key].get<bool>();
    return def;
}
static inline std::vector<int> ivec(const json& j, const char* key) {
    std::vector<int> v;
    if (!j.contains(key) || !j[key].is_array()) return v;
    for (auto& e : j[key]) {
        if (e.is_number_integer() || e.is_number_unsigned()) v.push_back(e.get<int>());
    }
    return v;
}

// Merge "entities" and "extended_entities" similar to your Python
static json merge_entities(json entities, const json& extended_entities) {
    if (extended_entities.is_null()) return entities.is_null() ? json::object() : entities;
    if (entities.is_null()) entities = json::object();

    for (auto it = extended_entities.begin(); it != extended_entities.end(); ++it) {
        const std::string key = it.key();
        const json& ext_val = it.value();
        if (ext_val.is_array()) {
            std::unordered_set<long long> seen_ids;
            json merged_list = json::array();
            const json& base = entities.contains(key) ? entities[key] : json::array();
            for (const json* src : {&base, &ext_val}) {
                if (!src->is_array()) continue;
                for (const auto& item : *src) {
                    long long mid = 0;
                    if (item.is_object() && item.contains("id") &&
                        (item["id"].is_number_integer() || item["id"].is_number_unsigned())) {
                        mid = item["id"].get<long long>();
                        if (seen_ids.insert(mid).second) merged_list.push_back(item);
                    } else {
                        merged_list.push_back(item);
                    }
                }
            }
            entities[key] = merged_list;
        } else {
            entities[key] = ext_val;
        }
    }
    return entities;
}

// ISO-ish time passthrough (Twitter already provides ISO/RFC times typically)
static inline std::string to_iso(const std::string& input) {
    // Example input: "Mon Aug 10 05:11:31 +0000 2020"
    std::tm tm{};
    std::istringstream ss(input);
    std::string wkday, month, tz;
    int day, year;
    ss >> wkday >> month >> day;
    ss >> std::get_time(&tm, "%H:%M:%S"); // parse time
    ss >> tz >> year;
    if (ss.fail()) return ""; // parsing failed

    // Convert month string to month number
    static const std::map<std::string, int> months = {
        {"Jan",1},{"Feb",2},{"Mar",3},{"Apr",4},{"May",5},{"Jun",6},
        {"Jul",7},{"Aug",8},{"Sep",9},{"Oct",10},{"Nov",11},{"Dec",12}
    };
    auto it = months.find(month);
    if (it == months.end()) return "";

    tm.tm_mday = day;
    tm.tm_mon = it->second - 1;
    tm.tm_year = year - 1900;

    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");

    // Handle timezone: "+0000" -> "+00:00"
    if (tz.size() == 5 && (tz[0] == '+' || tz[0] == '-'))
        out << tz.substr(0,3) << ":" << tz.substr(3);
    else
        out << "+00:00"; // fallback if missing

    return out.str();
}

// ------------------------------ Per-file processing ------------------------------
struct ThreadTables {
    std::vector<std::vector<std::string>> users;
    std::vector<std::vector<std::string>> places;
    std::vector<std::vector<std::string>> tweets;
    std::vector<std::vector<std::string>> tweet_hashtags;
    std::vector<std::vector<std::string>> urls;
    std::vector<std::vector<std::string>> media;
    std::vector<std::vector<std::string>> user_mentions;

    void clear_all() {
        users.clear(); places.clear(); tweets.clear(); tweet_hashtags.clear();
        urls.clear(); media.clear(); user_mentions.clear();
    }
};

static void parse_tweet(const json& t, ThreadTables& tb);

// Main per-file worker
static void process_file(const fs::path& tweets_file_path, std::optional<int> max_line_opt)
{
    using clock = std::chrono::steady_clock;
    auto t0 = clock::now();

    ThreadTables tb;
    std::string base_name = tweets_file_path.filename().string();
    // mimic Python slice [29:] if your filenames have a fixed prefix; here we keep as-is:
    std::string base_file_name = fs::path(base_name).replace_extension().string();

    std::ifstream in(tweets_file_path);
    if (!in) {
        LOG.error("Error opening file: ", tweets_file_path.string());
        return;
    }

    std::string line;
    int line_count = 0;

    auto flush_batch = [&](){
        auto io_start = clock::now();
        const std::vector<std::pair<std::string, const std::vector<std::vector<std::string>>*>> tables = {
            {"users", &tb.users}, {"places", &tb.places}, {"tweets", &tb.tweets},
            {"tweet_hashtag", &tb.tweet_hashtags}, {"urls", &tb.urls}, {"media", &tb.media},
            {"user_mentions", &tb.user_mentions}
        };
        for (auto& [name, vec] : tables) {
            if (vec->empty()) continue;
            csv_write_rows("output/" + base_file_name + "_" + name + ".csv", *vec);
        }
        auto io_stop = clock::now();
        io_time_sec += std::chrono::duration<double>(io_stop - io_start).count();
        tb.clear_all();
    };

    try {
        while (std::getline(in, line)) {
            if (line.find_first_not_of(" \t\r\n") == std::string::npos) continue;
            ++line_count;
            if (max_line_opt && *max_line_opt > 0 && line_count > *max_line_opt) break;

            json tweet_json;
            try { tweet_json = json::parse(line); }
            catch (const std::exception& e) {
                LOG.error("Error parsing JSON: ", e.what());
                break;
            }

            if (tweet_json.contains("extended_entities"))
                tweet_json["entities"] = merge_entities(tweet_json.value("entities", json::object()),
                                                       tweet_json["extended_entities"]);

            try {
                parse_tweet(tweet_json, tb);
            } catch (const std::exception& e) {
                LOG.error("Error parsing tweet object: ", e.what());
                break;
            }

            if (line_count % BATCH_SIZE == 0) flush_batch();
        }
        flush_batch(); // remainder
    } catch (const std::exception& e) {
        LOG.error("Error processing file ", tweets_file_path.string(), ": ", e.what());
        return;
    }

    auto t1 = clock::now();
    LOG.info("Processed ", (line_count), " tweets from ", base_name,
             " in ", std::chrono::duration<double>(t1 - t0).count(), " seconds.");
}

// Extracts and appends to per-thread tables with global dedupe
static void parse_tweet(const json& _tweet, ThreadTables& tb)
{
    // --- users (sender)
    const json& user = _tweet.contains("user") ? _tweet["user"] : json::object();
    long long user_id = iget_ll(user, "id");
    {
        // missing_mentioned_users removal if present
        {
            std::unique_lock<std::shared_mutex> lk(missing_users_mtx);
            if (missing_mentioned_users_set.erase(user_id) > 0) {
                // removed from incomplete list
            }
        }

        bool insert = false;
        {
            std::shared_lock<std::shared_mutex> lk(users_mtx);
            insert = (users_set.find(user_id) == users_set.end());
        }
        if (insert) {
            std::unique_lock<std::shared_mutex> lk(users_mtx);
            if (users_set.insert(user_id).second) {
                tb.users.push_back({
                    std::to_string(user_id),
                    sget(user, "screen_name"),
                    sget(user, "name"),
                    sget(user, "description"),
                    user.contains("verified") ? (bget(user,"verified") ? "true" : "false") : "",
                    user.contains("protected") ? (bget(user,"protected") ? "true" : "false") : "",
                    user.contains("followers_count") ? std::to_string(iget_ll(user,"followers_count")) : "",
                    user.contains("friends_count") ? std::to_string(iget_ll(user,"friends_count")) : "",
                    user.contains("statuses_count") ? std::to_string(iget_ll(user,"statuses_count")) : "",
                    to_iso(sget(user,"created_at")),
                    sget(user,"location"),
                    sget(user,"url")
                });
            }
        }
    }

    // --- place
    if (_tweet.contains("place") && !_tweet["place"].is_null()) {
        const json& plc = _tweet["place"];
        std::string pid = sget(plc, "id");
        bool insert = false;
        {
            std::shared_lock<std::shared_mutex> lk(places_mtx);
            insert = (places_set.find(pid) == places_set.end());
        }
        if (insert) {
            std::unique_lock<std::shared_mutex> lk(places_mtx);
            if (places_set.insert(pid).second) {
                tb.places.push_back({
                    pid,
                    sget(plc,"full_name"),
                    sget(plc,"country"),
                    sget(plc,"country_code"),
                    sget(plc,"place_type")
                });
            }
        }
    }

    // --- tweets
    long long tid = iget_ll(_tweet, "id");
    {
        bool insert = false;
        {
            std::shared_lock<std::shared_mutex> lk(tweets_mtx);
            insert = (tweets_set.find(tid) == tweets_set.end());
        }
        if (insert) {
            std::unique_lock<std::shared_mutex> lk(tweets_mtx);
            if (tweets_set.insert(tid).second) {
                auto dtr = ivec(_tweet, "display_text_range");
                std::string d0 = dtr.size() > 0 ? std::to_string(dtr[0]) : "";
                std::string d1 = dtr.size() > 1 ? std::to_string(dtr[1]) : "";
                tb.tweets.push_back({
                    std::to_string(tid),
                    to_iso(sget(_tweet,"created_at")),
                    sget(_tweet,"full_text"),
                    d0, d1,
                    sget(_tweet,"lang"),
                    user_id ? std::to_string(user_id) : "",
                    sget(_tweet,"source"),
                    std::to_string(iget_ll(_tweet,"in_reply_to_status_id")),
                    std::to_string(iget_ll(_tweet,"quoted_status_id")),
                    _tweet.contains("retweeted_status") && _tweet["retweeted_status"].contains("id")
                        ? std::to_string(iget_ll(_tweet["retweeted_status"],"id")) : "",
                    _tweet.contains("place") ? sget(_tweet["place"],"id") : "",
                    _tweet.contains("retweet_count") ? std::to_string(iget_ll(_tweet,"retweet_count")) : "",
                    _tweet.contains("favorite_count") ? std::to_string(iget_ll(_tweet,"favorite_count")) : "",
                    _tweet.contains("possibly_sensitive") ? (bget(_tweet,"possibly_sensitive") ? "true":"false") : ""
                });
            }
        }
    }

    const json& entities = _tweet.contains("entities") ? _tweet["entities"] : json::object();

    // --- hashtags
    if (entities.contains("hashtags") && entities["hashtags"].is_array()) {
        for (const auto& h : entities["hashtags"]) {
            std::string tag = sget(h, "text");
            std::transform(tag.begin(), tag.end(), tag.begin(), ::tolower);
            int hid;
            {
                std::lock_guard<std::mutex> lk(hashtags_mtx);
                auto it = hashtags_map.find(tag);
                if (it == hashtags_map.end()) {
                    hid = curr_hashtag_id++;
                    hashtags_map.emplace(tag, hid);
                } else {
                    hid = it->second;
                }
            }
            {
                std::shared_lock<std::shared_mutex> lk(tweet_hashtags_mtx);
                if (tweet_hashtags_set.find({tid, hid}) == tweet_hashtags_set.end()) {
                    lk.unlock();
                    std::unique_lock<std::shared_mutex> ulk(tweet_hashtags_mtx);
                    if (tweet_hashtags_set.insert({tid,hid}).second) {
                        tb.tweet_hashtags.push_back({ std::to_string(tid), std::to_string(hid) });
                    }
                }
            }
        }
    }

    // --- urls
    if (entities.contains("urls") && entities["urls"].is_array()) {
        for (const auto& u : entities["urls"]) {
            std::string url = sget(u,"url");
            std::shared_lock<std::shared_mutex> lk(urls_mtx);
            if (urls_set.find({tid, url}) == urls_set.end()) {
                lk.unlock();
                std::unique_lock<std::shared_mutex> ulk(urls_mtx);
                if (urls_set.insert({tid,url}).second) {
                    tb.urls.push_back({
                        std::to_string(tid),
                        sget(u,"url"),
                        sget(u,"expanded_url"),
                        sget(u,"display_url"),
                        sget(u,"unwound_url")
                    });
                }
            }
        }
    }

    // --- media
    if (entities.contains("media") && entities["media"].is_array()) {
        for (const auto& m : entities["media"]) {
            long long mid = iget_ll(m, "id");
            std::shared_lock<std::shared_mutex> lk(media_mtx);
            if (media_set.find({tid, mid}) == media_set.end()) {
                lk.unlock();
                std::unique_lock<std::shared_mutex> ulk(media_mtx);
                if (media_set.insert({tid, mid}).second) {
                    tb.media.push_back({
                        std::to_string(tid),
                        mid ? std::to_string(mid) : "",
                        sget(m,"type"),
                        sget(m,"media_url"),
                        sget(m,"media_url_https"),
                        sget(m,"display_url"),
                        sget(m,"expanded_url")
                    });
                }
            }
        }
    }

    // --- user mentions
    if (entities.contains("user_mentions") && entities["user_mentions"].is_array()) {
        for (const auto& um : entities["user_mentions"]) {
            long long mid = iget_ll(um, "id");
            {
                std::shared_lock<std::shared_mutex> lk(user_mentions_mtx);
                if (user_mentions_set.find({mid, tid}) == user_mentions_set.end()) {
                    lk.unlock();
                    std::unique_lock<std::shared_mutex> ulk(user_mentions_mtx);
                    if (user_mentions_set.insert({mid, tid}).second) {
                        tb.user_mentions.push_back({
                            std::to_string(tid),
                            mid ? std::to_string(mid) : "",
                            sget(um,"screen_name"),
                            sget(um,"name")
                        });
                    }
                }
            }
            // create minimal user rows for mentioned users if missing
            bool need_stub = false;
            {
                std::shared_lock<std::shared_mutex> lk(users_mtx);
                need_stub = (users_set.find(mid) == users_set.end());
            }
            if (need_stub) {
                {
                    std::unique_lock<std::shared_mutex> lk(users_mtx);
                    if (users_set.insert(mid).second) {
                        tb.users.push_back({
                            mid ? std::to_string(mid) : "",
                            sget(um,"screen_name"),
                            sget(um,"name"),
                            "", "", "", "0","0","0", "", "", ""
                        });
                    }
                }
                {
                    std::unique_lock<std::shared_mutex> lk(missing_users_mtx);
                    missing_mentioned_users_set.insert(mid);
                }
            }
        }
    }

    // --- nested tweets
    if (_tweet.contains("quoted_status") && _tweet["quoted_status"].is_object())
        parse_tweet(_tweet["quoted_status"], tb);
    if (_tweet.contains("retweeted_status") && _tweet["retweeted_status"].is_object())
        parse_tweet(_tweet["retweeted_status"], tb);
}

// ------------------------------ Main ------------------------------
int main(int argc, char* argv[]) {
    try {
        fs::create_directories("output");
        const fs::path data_dir = (argc > 1) ? fs::path(argv[1]) : R"(C:\Users\marti\PycharmProjects\PDT\data)";

        // find *.jsonl
        std::vector<fs::path> jsonl_files;
        for (auto& entry : fs::directory_iterator(data_dir)) {
            if (!entry.is_regular_file()) continue;
            if (entry.path().extension() == ".jsonl")
                jsonl_files.push_back(entry.path());
        }

        // clean partial CSVs first (like Python)
        {
            auto io_start = std::chrono::steady_clock::now();
            for (const auto& file_path : jsonl_files) {
                std::string base = file_path.filename().string();
                std::string base_no_ext = fs::path(base).replace_extension().string();
                for (const char* table : {"users","places","tweets","tweet_hashtag","urls","media","user_mentions"}) {
                    fs::path csv = fs::path("output") / (base_no_ext + std::string("_") + table + ".csv");
                    if (fs::exists(csv)) fs::remove(csv);
                }
            }
            auto io_stop = std::chrono::steady_clock::now();
            io_time_sec += std::chrono::duration<double>(io_stop - io_start).count();
        }

        auto total_start = std::chrono::steady_clock::now();

        // Thread pool via futures
        {
            std::vector<std::future<void>> futs;
            size_t idx = 0;
            // Simple work distributor
            for (const auto& p : jsonl_files) {
                futs.emplace_back(std::async(std::launch::async, [p](){
                    process_file(p, 10000000000000); // match your Python cap
                }));
                if (futs.size() >= static_cast<size_t>(WORKER_COUNT)) {
                    for (auto& f : futs) f.get();
                    futs.clear();
                }
            }
            for (auto& f : futs) f.get();
        }

        // join per-file CSVs to final tables and cleanup partials
        {
            auto io_start = std::chrono::steady_clock::now();
            for (const char* table : {"users","places","tweets","tweet_hashtag","urls","media","user_mentions"}) {
                std::ofstream out(std::string("output/") + table + ".csv", std::ios::binary);
                for (const auto& file_path : jsonl_files) {
                    std::string base = file_path.filename().string();
                    std::string base_no_ext = fs::path(base).replace_extension().string();
                    fs::path partial = fs::path("output") / (base_no_ext + std::string("_") + table + ".csv");
                    if (!fs::exists(partial)) continue;
                    std::ifstream in(partial, std::ios::binary);
                    out << in.rdbuf();
                    in.close();
                    fs::remove(partial);
                }
            }

            // temp_users.csv (IDs of mentioned-only/incomplete users)
            {
                std::ofstream tmp("output/temp_users.csv");
                std::shared_lock<std::shared_mutex> lk(missing_users_mtx);
                for (auto uid : missing_mentioned_users_set) tmp << uid << "\n";
            }

            // hashtags.csv (id, hashtag)
            {
                std::ofstream hf("output/hashtags.csv");
                std::lock_guard<std::mutex> lk(hashtags_mtx);
                for (const auto& kv : hashtags_map) {
                    hf << kv.second << "," << csv_quote(kv.first) << "\n";
                }
            }
            auto io_stop = std::chrono::steady_clock::now();
            io_time_sec += std::chrono::duration<double>(io_stop - io_start).count();
        }

        auto total_stop = std::chrono::steady_clock::now();
        double total_sec = std::chrono::duration<double>(total_stop - total_start).count();

        // stats
        {
            std::shared_lock<std::shared_mutex> a(users_mtx), b(places_mtx), c(tweets_mtx),
                                                d(tweet_hashtags_mtx), e(urls_mtx),
                                                f(media_mtx), g(user_mentions_mtx), h(missing_users_mtx);
            LOG.info("All files processed in ", total_sec, " seconds.");
            LOG.info("Unique users: ", users_set.size(),
                     ", places: ", places_set.size(),
                     ", tweets: ", tweets_set.size(),
                     ", hashtags: ", hashtags_map.size(),
                     ", urls: ", urls_set.size(),
                     ", media: ", media_set.size(),
                     ", user_mentions: ", user_mentions_set.size(),
                     ", incomplete users born from user_mentions: ", missing_mentioned_users_set.size());
            const double io = io_time_sec.load();
            LOG.info("Total IO time: ", io, " seconds. Ratio of compute/io is ",
                     (io > 0.0 ? (total_sec - io) / io : std::numeric_limits<double>::infinity()));
        }
    } catch (const std::exception& e) {
        LOG.error("Fatal error: ", e.what());
        return 1;
    }
    return 0;
}
