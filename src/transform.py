# Beautiful RSS - serverless transform (Python). TRMNL caps polled data at ~100kB
# combined, so we fetch the feeds here (this runtime has network; polling doesn't
# help) and return only the merged, newest-first, trimmed `stories`. Feeds come
# from three preset selectors + the `feed_urls` multi-string field (combined,
# deduped); polling_url is just a heartbeat that triggers run().
# Scope: RSS 2.0. Output: stories ["EPOCH|||TITLE|||IMG|||SUMMARY|||OUTLET|||DATE|||END", ...]

import base64
import json
import re
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET


MAX_STORIES = 5
MAX_TITLE_CHARS = 240
MAX_SUMMARY_CHARS = 600
MAX_IMAGE_URL_CHARS = 1500
MAX_OUTLET_CHARS = 40
MAX_DATE_CHARS = 100
# TRMNL rejects transformed merge variables at 100 kB. Keep a 20 kB margin
# for JSON escaping, metadata, and the optional embedded single-feed icon.
MAX_OUTPUT_BYTES = 80000
MIN_SUMMARY_CHARS = 120
FEED_PRESETS = {
    # Essentials
    'bbc-news-top': 'https://feeds.bbci.co.uk/news/rss.xml',
    'nytimes-world': 'https://rss.nytimes.com/services/xml/rss/nyt/World.xml',
    'guardian-world': 'https://www.theguardian.com/world/rss',
    'fox-news-latest': 'https://moxie.foxnews.com/google-publisher/latest.xml',
    'abc-news-us': 'https://feeds.abcnews.com/abcnews/topstories',
    'wsj-world-news': 'https://feeds.content.dowjones.io/public/rss/RSSWorldNews',
    'financial-times-home': 'https://www.ft.com/rss/home',
    'sky-news-world': 'https://feeds.skynews.com/feeds/rss/world.xml',
    # Topics: technology
    'bbc-news-technology': 'https://feeds.bbci.co.uk/news/technology/rss.xml',
    'wired': 'https://www.wired.com/feed/rss',
    'ars-technica': 'https://feeds.arstechnica.com/arstechnica/index',
    'ieee-spectrum': 'https://spectrum.ieee.org/feeds/feed.rss',
    '404-media': 'https://www.404media.co/rss/',
    'rest-of-world': 'https://restofworld.org/feed/latest/',
    # Topics: science and environment
    'bbc-news-science-environment': 'https://feeds.bbci.co.uk/news/science_and_environment/rss.xml',
    'nytimes-climate': 'https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml',
    'mongabay': 'https://news.mongabay.com/feed/',
    # Topics: space
    'nasa-news-releases': 'https://www.nasa.gov/news-release/feed/',
    'space-com': 'https://www.space.com/feeds/all',
    'space-news': 'https://spacenews.com/feed/',
    # Topics: sports
    'bbc-sport-top': 'https://feeds.bbci.co.uk/sport/rss.xml',
    'sky-sports-news': 'https://www.skysports.com/rss/12040',
    'bbc-sport-formula-1': 'https://feeds.bbci.co.uk/sport/formula1/rss.xml',
    # Topics: crypto
    'watcher-guru': 'https://watcher.guru/news/feed',
    # Topics: music, arts, and culture
    'pitchfork-news': 'https://pitchfork.com/feed/feed-news/rss',
    'colossal': 'https://www.thisiscolossal.com/feed/',
    'hyperallergic': 'https://hyperallergic.com/feed/',
    # Regional
    'elpais-english': 'https://feeds.elpais.com/mrss-s/pages/ep/site/english.elpais.com/portada',
    'le-monde-world-english': 'https://www.lemonde.fr/en/international/rss_full.xml',
    'guardian-australia': 'https://www.theguardian.com/australia-news/rss',
    'global-news-canada': 'https://globalnews.ca/canada/feed/',
    'france24-english': 'https://www.france24.com/en/rss',
    'tagesschau-germany': 'https://www.tagesschau.de/infoservices/alle-meldungen-100~rss2.xml',
    'nos-netherlands': 'https://feeds.nos.nl/nosnieuwsalgemeen',
    'dn-sweden': 'https://www.dn.se/rss/',
    'cna-asia': 'https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6511',
    'soranews24': 'https://soranews24.com/feed/',
    'africanews': 'https://www.africanews.com/feed/rss',
    'ceske-noviny': 'https://www.ceskenoviny.cz/sluzby/rss/zpravy.php',
    'buenos-aires-times': 'https://www.batimes.com.ar/feed',
    'balkan-insight': 'https://balkaninsight.com/feed/',
    # Backward compatibility for the former Popular feeds selector. TRMNL
    # lowercases select values, so restore the case-sensitive WSJ path.
    'https://feeds.content.dowjones.io/public/rss/rssopinion':
        'https://feeds.content.dowjones.io/public/rss/RSSOpinion',
}

PRESET_ABBREVIATIONS = {
    # Essentials
    'bbc-news-top': 'BBC',
    'nytimes-world': 'NYT',
    'guardian-world': 'GUARDIAN',
    'fox-news-latest': 'FOX',
    'abc-news-us': 'ABC',
    'wsj-world-news': 'WSJ',
    'financial-times-home': 'FT',
    'sky-news-world': 'SKY',
    # Topics: technology
    'bbc-news-technology': 'BBC',
    'wired': 'WIRED',
    'ars-technica': 'ARS',
    'ieee-spectrum': 'IEEE',
    '404-media': '404',
    'rest-of-world': 'ROW',
    # Topics: science and environment
    'bbc-news-science-environment': 'BBC',
    'nytimes-climate': 'NYT',
    'mongabay': 'MONGABAY',
    # Topics: space
    'nasa-news-releases': 'NASA',
    'space-com': 'SPACE.COM',
    'space-news': 'SPACENEWS',
    # Topics: sports
    'bbc-sport-top': 'BBC',
    'sky-sports-news': 'SKY SPORTS',
    'bbc-sport-formula-1': 'BBC F1',
    # Topics: crypto
    'watcher-guru': 'WATCHER',
    # Topics: music, arts, and culture
    'pitchfork-news': 'PITCHFORK',
    'colossal': 'COLOSSAL',
    'hyperallergic': 'HYPERALLERGIC',
    # Regional
    'elpais-english': 'EL PAÍS',
    'le-monde-world-english': 'LE MONDE',
    'guardian-australia': 'GUARDIAN',
    'global-news-canada': 'GLOBAL',
    'france24-english': 'FRANCE 24',
    'tagesschau-germany': 'TAGESSCHAU',
    'nos-netherlands': 'NOS',
    'dn-sweden': 'DN',
    'cna-asia': 'CNA',
    'soranews24': 'SORA24',
    'africanews': 'AFRICANEWS',
    'ceske-noviny': 'ČN',
    'buenos-aires-times': 'BA TIMES',
    'balkan-insight': 'BIRN',
}

PRESET_TITLES = {
    # Essentials
    'bbc-news-top': 'BBC News',
    'nytimes-world': 'The New York Times',
    'guardian-world': 'The Guardian',
    'fox-news-latest': 'Fox News',
    'abc-news-us': 'ABC News',
    'wsj-world-news': 'Wall Street Journal',
    'financial-times-home': 'Financial Times',
    'sky-news-world': 'Sky News',
    # Topics: technology
    'bbc-news-technology': 'BBC News',
    'wired': 'Wired',
    'ars-technica': 'Ars Technica',
    'ieee-spectrum': 'IEEE Spectrum',
    '404-media': '404 Media',
    'rest-of-world': 'Rest of World',
    # Topics: science and environment
    'bbc-news-science-environment': 'BBC News',
    'nytimes-climate': 'The New York Times',
    'mongabay': 'Mongabay',
    # Topics: space
    'nasa-news-releases': 'NASA',
    'space-com': 'Space.com',
    'space-news': 'SpaceNews',
    # Topics: sports
    'bbc-sport-top': 'BBC Sport',
    'sky-sports-news': 'Sky Sports',
    'bbc-sport-formula-1': 'BBC Sport — Formula 1',
    # Topics: crypto
    'watcher-guru': 'Watcher.Guru',
    # Topics: music, arts, and culture
    'pitchfork-news': 'Pitchfork',
    'colossal': 'Colossal',
    'hyperallergic': 'Hyperallergic',
    # Regional
    'elpais-english': 'El País',
    'le-monde-world-english': 'Le Monde',
    'guardian-australia': 'Guardian Australia',
    'global-news-canada': 'Global News',
    'france24-english': 'France 24',
    'tagesschau-germany': 'Tagesschau',
    'nos-netherlands': 'NOS',
    'dn-sweden': 'Dagens Nyheter',
    'cna-asia': 'CNA',
    'soranews24': 'SoraNews24',
    'africanews': 'Africanews',
    'ceske-noviny': 'České noviny',
    'buenos-aires-times': 'Buenos Aires Times',
    'balkan-insight': 'Balkan Insight',
}

PRESET_URL_ABBREVIATIONS = {
    FEED_PRESETS[key]: outlet
    for key, outlet in PRESET_ABBREVIATIONS.items()
}


def run(input):
    specs = _feed_specs(input)
    channels = _fetch_all(specs)

    records = []
    for ch, outlet, _ in channels:
        for item in _kids(ch, 'item'):
            rec = _record(item, outlet)
            if rec:
                records.append(rec)
    records.sort(key=lambda r: r[0], reverse=True)
    stories = [r[1] for r in records[:MAX_STORIES]]

    if not stories:
        stories = [_rec(0, 'Beautiful RSS', '', 'Choose one or more feeds in the plugin settings.', '', '')]

    single = len(specs) == 1
    single_channel = channels[0] if single and channels else None
    result = {
        'stories': stories,
        'single_feed': single,
        'feed_title_src': (
            single_channel[2] or _text(_one(single_channel[0], 'title'))
            if single_channel else ''
        ),
        'feed_icon_src': (
            _icon_data_uri(_url(_one(single_channel[0], 'image')))
            if single_channel else ''
        ),
    }
    return _fit_output_budget(result)


def _feed_specs(input):
    try:
        cf = input['trmnl']['plugin_settings']['custom_fields_values']
    except (KeyError, TypeError):
        return list()

    seen, specs = set(), []
    selected = []
    for key in (
        'feed_presets_essentials',
        'feed_presets_topics',
        'feed_presets_regional',
        'feed_presets',  # legacy field
    ):
        selected.extend(_split(cf.get(key)))

    custom = _split(cf.get('feed_urls')) + _split(cf.get('feed_url'))  # legacy field
    entries = [(value, True) for value in selected]
    entries.extend((value, False) for value in custom)
    for value, from_preset_selector in entries:
        key = _preset_key(value)
        url = FEED_PRESETS.get(key, value)
        if url not in seen:
            seen.add(url)
            outlet = (
                PRESET_ABBREVIATIONS.get(key)
                or PRESET_URL_ABBREVIATIONS.get(url)
                or ''
            )
            curated_title = PRESET_TITLES.get(key, '') if from_preset_selector else ''
            specs.append((url, outlet, curated_title))
    return specs


def _feed_urls(input):
    return [spec[0] for spec in _feed_specs(input)]


def _preset_key(value):
    key = value.lower().strip()
    if ':' in key:
        # The first version of the multi-select options used "Label: value"
        # scalars. TRMNL stored the whole normalized label. Keep existing
        # plugin instances working after the options are corrected to maps.
        legacy_suffix = key.rsplit(':', 1)[-1].lstrip('_ ')
        if legacy_suffix in FEED_PRESETS:
            return legacy_suffix
    return key


def _split(value):
    items = value if isinstance(value, list) else re.split(r'[\r\n,]+', value or '')
    return [str(i).strip() for i in items if str(i).strip()]


def _fetch_all(specs):
    if not specs:
        return list()
    with ThreadPoolExecutor(max_workers=8) as pool:
        roots = list(pool.map(lambda spec: _fetch(spec[0]), specs))
    channels = []
    for root, (_, preset_outlet, curated_title) in zip(roots, specs):
        if root is None:
            continue
        channel = _one(root, 'channel') or root
        outlet = preset_outlet or _abbreviation(_text(_one(channel, 'title')))
        channels.append((channel, outlet, curated_title))
    return channels


def _fetch(url):
    try:
        import requests  # lazy: module still loads where requests is absent
        r = requests.get(
            url,
            timeout=4,
            headers={'User-Agent': 'TRMNL Beautiful RSS'},
        )
        if not r.ok or not r.content:
            return None
        return ET.fromstring(r.content)
    except Exception:
        return None


def _icon_data_uri(url):
    if not url:
        return ''
    try:
        import requests
        response = requests.get(
            url,
            timeout=4,
            headers={'User-Agent': 'TRMNL Beautiful RSS'},
        )
        content = response.content if response.ok else b''
        mime = response.headers.get('content-type', '').split(';', 1)[0].lower()
        if not mime.startswith('image/') or not content or len(content) > 24000:
            return ''
        encoded = base64.b64encode(content).decode('ascii')
        return 'data:{};base64,{}'.format(mime, encoded)
    except Exception:
        return ''


def _record(item, outlet):
    title = _clip_text(_text(_one(item, 'title')), MAX_TITLE_CHARS)
    img = _bounded_url(_img(item))
    summary = _clip_text(_summary(item), MAX_SUMMARY_CHARS)
    date = _clip_text(
        _text(_one(item, 'pubDate')) or _text(_one(item, 'date')),
        MAX_DATE_CHARS,
    )
    if not (title or img or summary):
        return None
    epoch = _epoch(date)
    return epoch, _rec(
        epoch,
        title,
        img,
        summary,
        _clip_text(outlet, MAX_OUTLET_CHARS),
        date,
    )


def _abbreviation(title):
    title = _clean(title)
    if len(title) <= 20:
        return title
    words = re.findall(r'[^\W_]+', title, flags=re.UNICODE)
    if not words:
        return ''
    if len(words) == 1:
        return words[0][:6].upper()
    return ''.join(word[0] for word in words[:6]).upper()


def _img(item):
    for name in ('content', 'thumbnail', 'enclosure', 'image'):
        url = _url(_one(item, name))
        if url:
            return url
    m = re.search(r'src="([^"]+)"', _text(_one(item, 'encoded')) or _text(_one(item, 'description')))
    return m.group(1) if m else ''


def _summary(item):
    text = _text(_one(item, 'description')) or _text(_one(item, 'encoded'))
    return re.sub(r'<[^>]*>', ' ', text).split('The post')[0]


def _kids(parent, name):
    return [c for c in parent if c.tag.rsplit('}', 1)[-1] == name] if parent is not None else list()


def _one(parent, name):
    kids = _kids(parent, name)
    return kids[0] if kids else None


def _text(el):
    return (el.text or '').strip() if el is not None else ''


def _url(el):
    if el is None:
        return ''
    return (el.get('url') or _text(_one(el, 'url'))).strip()


def _rec(epoch, title, img, summary, outlet, date):
    return '|||'.join([str(epoch), title, img, summary, outlet, date]) + '|||END'


def _clean(s):
    return re.sub(r'\s+', ' ', (s or '').replace('|||', '/')).strip()


def _clip_text(value, limit):
    value = _clean(value)
    if len(value) <= limit:
        return value
    clipped = value[:limit - 1]
    word_boundary = clipped.rfind(' ')
    if word_boundary >= limit // 2:
        clipped = clipped[:word_boundary]
    return clipped.rstrip(' ,;:-') + '…'


def _bounded_url(value):
    value = _clean(value)
    return value if len(value) <= MAX_IMAGE_URL_CHARS else ''


def _output_size(result):
    return len(
        json.dumps(
            result,
            ensure_ascii=True,
            separators=(',', ':'),
        ).encode('utf-8')
    )


def _fit_output_budget(result):
    stories = result.get('stories') or []
    while stories and _output_size(result) > MAX_OUTPUT_BYTES:
        fields = [story.split('|||') for story in stories]
        candidates = [
            (len(parts[3]), index)
            for index, parts in enumerate(fields)
            if len(parts) >= 7 and len(parts[3]) > MIN_SUMMARY_CHARS
        ]
        if candidates:
            length, index = max(candidates)
            fields[index][3] = _clip_text(
                fields[index][3],
                max(MIN_SUMMARY_CHARS, length - 160),
            )
            stories[index] = '|||'.join(fields[index])
        else:
            # Oldest story is last because records are sorted newest-first.
            stories.pop()
    return result


def _epoch(s):
    try:
        return int(parsedate_to_datetime(s).timestamp())
    except Exception:
        return 0