const map = L.map('map', {
    zoomControl: false,
    attributionControl: false
}).setView([37.5665, 126.9780], 11);

L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
    maxZoom: 19,
    subdomains: 'abcd',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
}).addTo(map);

L.control.attribution({ position: 'bottomright', prefix: false }).addTo(map);

const gpsLayer = L.layerGroup().addTo(map);

// 줌 12 미만: 구 단위 색칠 지도 / 12~15: 동 단위 색칠 지도 / 15 이상: 개별 마커·클러스터
const CHORO_MAX_ZOOM = 12;
const DONG_MAX_ZOOM = 15;
// 동 단위 색칠 지도 안에서도, 이 줌 미만은 폴리곤이 작아 숫자만 표시하고 이 줌 이상부터 동 이름도 표시한다
const DONG_NAME_MIN_ZOOM = 14;
let guChoroLayer = null;
let dongChoroLayer = null;

function choroColorScale(count, breaks) {
    if (count <= 0) return '#dfe6e9';
    if (count >= breaks[1]) return '#e74c3c';
    if (count >= breaks[0]) return '#f1c40f';
    return '#3498db';
}

function computeStatsBy(aggregated, keyFn) {
    const stats = {};
    aggregated.forEach(item => {
        const key = keyFn(item);
        stats[key] = (stats[key] || 0) + item.permit_count;
    });
    return stats;
}

function computeGuStats(aggregated) {
    return computeStatsBy(aggregated, item => item.address ? item.address.split(' ')[0] : '기타');
}

function computeDongStats(aggregated) {
    return computeStatsBy(aggregated, item => {
        if (!item.address) return '기타';
        const parts = item.address.split(' ');
        return parts.length >= 2 ? `${parts[0]} ${parts[1]}` : parts[0];
    });
}

function createChoroLayer(geojson, getKey, getLabel, extraTooltipClass) {
    const tooltipClass = extraTooltipClass ? `gu-tooltip ${extraTooltipClass}` : 'gu-tooltip';
    const group = L.geoJSON(geojson, {
        style: () => ({ color: '#fff', weight: 2, fillColor: '#dfe6e9', fillOpacity: 0.35 }),
        onEachFeature: (feature, layer) => {
            layer._statKey = getKey(feature);
            layer._labelName = getLabel(feature);
            layer.bindTooltip(
                `<span class="gu-name">${layer._labelName}</span><span class="gu-count">-<span class="gu-unit">건</span></span>`,
                { permanent: true, direction: 'center', className: tooltipClass, interactive: false }
            );
            layer.on('click', () => map.flyToBounds(layer.getBounds(), { padding: [20, 20] }));
            layer.on('mouseover', () => layer.setStyle({ weight: 3 }));
            layer.on('mouseout', () => layer.setStyle({ weight: 2 }));
        }
    });
    // hideEmpty로 개별 폴리곤을 껐다 켰다 하면 eachLayer로는 더 이상 안 잡히므로 따로 보관해둔다
    group._allLayers = [];
    group.eachLayer(featureLayer => group._allLayers.push(featureLayer));
    return group;
}

async function loadGuBoundaries() {
    const res = await fetch('./data/seoul_gu.geojson');
    const geojson = await res.json();
    guChoroLayer = createChoroLayer(geojson, f => f.properties.name, f => f.properties.name);
}

async function loadDongBoundaries() {
    const res = await fetch('./data/seoul_dong.geojson');
    const geojson = await res.json();
    dongChoroLayer = createChoroLayer(
        geojson,
        f => `${f.properties.gu} ${f.properties.EMD_KOR_NM}`,
        f => f.properties.EMD_KOR_NM,
        'dong-tooltip'
    );
}

function updateChoropleth(layer, stats, hideEmpty) {
    if (!layer) return;
    const values = Object.values(stats).filter(v => v > 0).sort((a, b) => a - b);
    const breaks = values.length
        ? [values[Math.floor(values.length / 3)], values[Math.floor(values.length * 2 / 3)] || values[values.length - 1]]
        : [1, 1];

    layer._allLayers.forEach(featureLayer => {
        const count = stats[featureLayer._statKey] || 0;

        if (hideEmpty && count <= 0) {
            if (layer.hasLayer(featureLayer)) layer.removeLayer(featureLayer);
            return;
        }
        if (!layer.hasLayer(featureLayer)) layer.addLayer(featureLayer);

        featureLayer.setStyle({ fillColor: choroColorScale(count, breaks), fillOpacity: count > 0 ? 0.65 : 0.35 });
        featureLayer.setTooltipContent(
            `<span class="gu-name">${featureLayer._labelName}</span><span class="gu-count">${count}<span class="gu-unit">건</span></span>`
        );
    });
}

function updateChoropleths() {
    updateChoropleth(guChoroLayer, computeGuStats(aggregatedCurrentData), false);
    // 동은 개수가 많아 0건까지 표시하면 라벨이 겹치므로 데이터 있는 동만 보여준다
    updateChoropleth(dongChoroLayer, computeDongStats(aggregatedCurrentData), true);
}

function toggleMapLayer(layer, shouldShow) {
    if (!layer) return;
    const isShown = map.hasLayer(layer);
    if (shouldShow && !isShown) map.addLayer(layer);
    if (!shouldShow && isShown) map.removeLayer(layer);
}

function updateZoomLayers() {
    const zoom = map.getZoom();
    toggleMapLayer(guChoroLayer, zoom < CHORO_MAX_ZOOM);
    toggleMapLayer(dongChoroLayer, zoom >= CHORO_MAX_ZOOM && zoom < DONG_MAX_ZOOM);
    map.getContainer().classList.toggle('map-hide-dong-name', zoom < DONG_NAME_MIN_ZOOM);

    const showMarkers = zoom >= DONG_MAX_ZOOM;
    Object.values(guClusterLayers).forEach(layer => toggleMapLayer(layer, showMarkers));
}

map.on('zoomend', updateZoomLayers);

let guClusterLayers = {};

function createClusterGroup() {
    return L.markerClusterGroup({
        chunkedLoading: true,
        maxClusterRadius: 60,
        disableClusteringAtZoom: 18,
        iconCreateFunction: function(cluster) {
            const children = cluster.getAllChildMarkers();
            let totalPermits = 0;
            children.forEach(marker => {
                totalPermits += (marker.options.permit_count || 0);
            });

            let c = 'custom-cluster-icon';
            let size = 40;
            if (totalPermits >= 50) { c += ' large'; size = 60; }
            else if (totalPermits >= 10) { c += ' medium'; size = 50; }

            return new L.DivIcon({
                html: `<div>${totalPermits}</div>`,
                className: c,
                iconSize: [size, size]
            });
        }
    });
}

let allData = [];
let aggregatedCurrentData = [];
let lastUpdatedDate = new Date();
let globalMarkers = {};

let currentRankLimit = 10;
let currentTab = 'overall';
let selectedDistrict = null;

async function loadData() {
    const loadingEl = document.getElementById('loading');
    loadingEl.style.display = 'block';

    try {
        const res = await fetch('./data/data.json');
        if (!res.ok) throw new Error('데이터 파일을 찾을 수 없습니다.');
        const json = await res.json();

        if (json.last_updated) {
            lastUpdatedDate = new Date(json.last_updated.replace(' ', 'T'));
            document.getElementById('last-updated').innerText = '업데이트: ' + json.last_updated;
        }

        allData = json.data.map(item => ({
            ...item,
            dateObj: parseDateString(item.date)
        }));

        setPeriod('60', document.getElementById('btn60'));

    } catch (err) {
        console.error(err);
        alert('데이터 로드 실패: ' + err.message);
    } finally {
        loadingEl.style.display = 'none';
    }
}

function toggleRankingPanel() {
    const panel = document.getElementById('rankingPanel');
    if (panel.style.display === 'flex') {
        panel.style.display = 'none';
    } else {
        panel.style.display = 'flex';
        renderRankingContent();
    }
}

function switchRankingTab(tabName) {
    currentTab = tabName;
    selectedDistrict = null;
    currentRankLimit = 10;

    document.querySelectorAll('.ranking-tab').forEach(el => el.classList.remove('active'));
    if(tabName === 'overall') {
        document.querySelectorAll('.ranking-tab')[0].classList.add('active');
    } else {
        document.querySelectorAll('.ranking-tab')[1].classList.add('active');
    }
    renderRankingContent();
}

function renderRankingContent() {
    const container = document.getElementById('rankingContent');
    container.innerHTML = '';

    if (currentTab === 'overall') {
        renderOverallRanking(container);
    } else {
        if (selectedDistrict) {
            renderDistrictDetail(container, selectedDistrict);
        } else {
            renderDistrictRanking(container);
        }
    }
}

function renderOverallRanking(container) {
    const sortedData = [...aggregatedCurrentData].sort((a, b) => b.permit_count - a.permit_count);
    const ul = document.createElement('ul');
    ul.className = 'rank-list';

    const limit = Math.min(currentRankLimit, sortedData.length);

    for (let i = 0; i < limit; i++) {
        const item = sortedData[i];
        const li = document.createElement('li');
        li.className = 'rank-item';
        li.onclick = () => {
            // 모바일에서 지도 볼 때 방해되지 않도록 선택 시 닫기 (선택 사항)
            document.getElementById('rankingPanel').style.display = 'none';
            const latKey = parseFloat(item.lat).toFixed(6);
            const lngKey = parseFloat(item.lng).toFixed(6);
            const key = `${latKey}|${lngKey}`;
            const target = globalMarkers[key];

            if (target) {
                target.clusterGroup.zoomToShowLayer(target.marker, function() {
                    target.marker.openPopup();
                });
            } else {
                map.flyTo([item.lat, item.lng], 17);
            }
        };

        const rankClass = (i < 3) ? 'top3' : '';
        li.innerHTML = `
            <span class="rank-num ${rankClass}">${i + 1}</span>
            <div class="rank-info">
                <span class="rank-name">${escapeHtml(item.place_name)}</span>
                <span class="rank-addr">${escapeHtml(item.address)}</span>
            </div>
            <span class="rank-count">${item.permit_count}건</span>
        `;
        ul.appendChild(li);
    }
    container.appendChild(ul);

    if (sortedData.length > limit) {
        const moreBtn = document.createElement('button');
        moreBtn.className = 'load-more-btn';
        moreBtn.innerText = '더보기 👇';
        moreBtn.onclick = () => {
            currentRankLimit += 10;
            renderRankingContent();
        };
        container.appendChild(moreBtn);
    }
}

function renderDistrictRanking(container) {
    const guStats = {};
    aggregatedCurrentData.forEach(item => {
        const gu = item.address ? item.address.split(' ')[0] : '기타';
        if (!guStats[gu]) guStats[gu] = 0;
        guStats[gu] += item.permit_count;
    });

    const sortedGu = Object.keys(guStats).sort((a, b) => guStats[b] - guStats[a]);

    const ul = document.createElement('ul');
    ul.className = 'rank-list';

    sortedGu.forEach((gu, idx) => {
        const li = document.createElement('li');
        li.className = 'rank-item';
        li.onclick = () => {
            selectedDistrict = gu;
            renderRankingContent();
        };

        const rankClass = (idx < 3) ? 'top3' : '';
        li.innerHTML = `
            <span class="rank-num ${rankClass}">${idx + 1}</span>
            <div class="rank-info">
                <span class="rank-name" style="font-size:16px;">${escapeHtml(gu)}</span>
            </div>
            <span class="rank-count">${guStats[gu]}건</span>
            <i class="fa-solid fa-chevron-right" style="margin-left:10px; color:#ccc;"></i>
        `;
        ul.appendChild(li);
    });
    container.appendChild(ul);
}

function renderDistrictDetail(container, districtName) {
    const header = document.createElement('div');
    header.className = 'back-btn-area';
    header.innerHTML = `<i class="fa-solid fa-arrow-left" style="margin-right:5px;"></i> ${escapeHtml(districtName)} 전체 목록`;
    header.onclick = () => {
        selectedDistrict = null;
        renderRankingContent();
    };
    container.appendChild(header);

    const districtData = aggregatedCurrentData
        .filter(item => item.address && item.address.startsWith(districtName))
        .sort((a, b) => b.permit_count - a.permit_count);

    const ul = document.createElement('ul');
    ul.className = 'rank-list';

    districtData.forEach((item, idx) => {
        const li = document.createElement('li');
        li.className = 'rank-item';
        li.onclick = () => {
            document.getElementById('rankingPanel').style.display = 'none';
            const latKey = parseFloat(item.lat).toFixed(6);
            const lngKey = parseFloat(item.lng).toFixed(6);
            const key = `${latKey}|${lngKey}`;
            const target = globalMarkers[key];

            if (target) {
                target.clusterGroup.zoomToShowLayer(target.marker, function() {
                    target.marker.openPopup();
                });
            } else {
                map.flyTo([item.lat, item.lng], 17);
            }
        };

        li.innerHTML = `
            <span class="rank-num">${idx + 1}</span>
            <div class="rank-info">
                <span class="rank-name">${escapeHtml(item.place_name)}</span>
                <span class="rank-addr">${escapeHtml(item.address)}</span>
            </div>
            <span class="rank-count">${item.permit_count}건</span>
        `;
        ul.appendChild(li);
    });
    container.appendChild(ul);
}

function toggleSearchMode(isSearch) {
    const filterGroup = document.getElementById('filterGroup');
    const searchGroup = document.getElementById('searchGroup');
    const searchInput = document.getElementById('searchInput');
    const searchResults = document.getElementById('searchResults');

    if (isSearch) {
        filterGroup.style.display = 'none';
        searchGroup.style.display = 'flex';
        searchInput.focus();
    } else {
        searchGroup.style.display = 'none';
        filterGroup.style.display = 'flex';
        searchResults.style.display = 'none';
        searchInput.value = '';
    }
}

function setPeriod(period, btn) {
    if (btn) {
        document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
    }

    let filteredData = [];

    if (period === 'all') {
        filteredData = allData;
    } else {
        const daysLimit = parseInt(period);
        filteredData = allData.filter(item => {
            if (!item.dateObj) return false;
            const diff = getDaysDiff(lastUpdatedDate, item.dateObj);
            return diff >= 0 && diff <= daysLimit;
        });
    }

    aggregatedCurrentData = aggregateData(filteredData);
    renderMarkers(aggregatedCurrentData);
    updateChoropleths();
    updateZoomLayers();

    if(document.getElementById('rankingPanel').style.display === 'flex') {
        currentRankLimit = 10;
        renderRankingContent();
    }
}

function aggregateData(points) {
    const mapData = {};

    points.forEach(pt => {
        if (!pt.lat || !pt.lng) return;
        const latKey = parseFloat(pt.lat).toFixed(6);
        const lngKey = parseFloat(pt.lng).toFixed(6);
        const key = `${latKey}|${lngKey}`;

        if (!mapData[key]) {
            mapData[key] = {
                lat: pt.lat, lng: pt.lng,
                address: pt.address, place_name: pt.place_name,
                permit_count: 0, history: []
            };
        }
        mapData[key].permit_count += 1;
        mapData[key].history.push({ date: pt.date, name: pt.place_name });
    });

    Object.values(mapData).forEach(item => {
        item.history.sort((a, b) => b.date.localeCompare(a.date));
    });
    return Object.values(mapData);
}

function buildPinIcon(pt) {
    const pinColor = pt.permit_count >= 10 ? '#e74c3c' : (pt.permit_count >= 3 ? '#f1c40f' : '#3498db');
    return L.divIcon({
        html: `
            <svg width="34" height="44" viewBox="0 0 34 44">
                <path d="M17 0C7.6 0 0 7.6 0 17c0 12.7 17 27 17 27s17-14.3 17-27C34 7.6 26.4 0 17 0z" fill="${pinColor}" stroke="white" stroke-width="2"/>
                <circle cx="17" cy="16" r="10.5" fill="white"/>
            </svg>
            <div class="pin-count">${pt.permit_count}</div>
        `,
        className: 'pin-marker',
        iconSize: [34, 44],
        iconAnchor: [17, 42],
        popupAnchor: [0, -40]
    });
}

function buildPopupContent(pt) {
    const historyHtml = pt.history.map(h => `
        <li class="permit-item">
            <span>${escapeHtml(h.name)}</span>
            <span class="permit-date">${formatDate(h.date)}</span>
        </li>
    `).join('');

    return `
        <div class="popup-header">
            <h3>${escapeHtml(pt.place_name)}</h3>
            <p>${escapeHtml(pt.address)}</p>
        </div>
        <div class="popup-body">
            <div class="stat-row">
                <span>📅 총 허가 건수</span>
                <strong style="color:#2c3e50;">${pt.permit_count}건</strong>
            </div>
            <ul class="permit-list">${historyHtml}</ul>
        </div>
    `;
}

// 필터(60/30/7일) 전환마다 클러스터/마커를 통째로 새로 만들지 않고,
// 이전에 그려둔 마커를 재사용해 사라진 것만 지우고 새로 생긴 것만 추가한다.
function renderMarkers(points) {
    const nextKeys = new Set();

    points.forEach(pt => {
        if (!pt.lat || !pt.lng) return;
        const latKey = parseFloat(pt.lat).toFixed(6);
        const lngKey = parseFloat(pt.lng).toFixed(6);
        const key = `${latKey}|${lngKey}`;
        nextKeys.add(key);

        const guName = pt.address ? pt.address.split(' ')[0] : '기타';
        if (!guClusterLayers[guName]) {
            guClusterLayers[guName] = createClusterGroup();
            map.addLayer(guClusterLayers[guName]);
        }

        const existing = globalMarkers[key];
        if (existing) {
            existing.marker.setIcon(buildPinIcon(pt));
            existing.marker.options.permit_count = pt.permit_count;
            existing.marker.setPopupContent(buildPopupContent(pt));
            return;
        }

        const marker = L.marker([pt.lat, pt.lng], {
            icon: buildPinIcon(pt),
            permit_count: pt.permit_count
        });
        marker.bindPopup(buildPopupContent(pt));
        guClusterLayers[guName].addLayer(marker);
        globalMarkers[key] = { marker: marker, clusterGroup: guClusterLayers[guName] };
    });

    Object.keys(globalMarkers).forEach(key => {
        if (nextKeys.has(key)) return;
        const { marker, clusterGroup } = globalMarkers[key];
        clusterGroup.removeLayer(marker);
        delete globalMarkers[key];
    });
}

const searchInput = document.getElementById('searchInput');
const searchResults = document.getElementById('searchResults');

searchInput.addEventListener('input', function(e) {
    const query = e.target.value.trim();
    if (query.length < 2) {
        searchResults.style.display = 'none';
        return;
    }

    const keywords = query.trim().split(/\s+/);

    const rawMatches = allData.filter(d => {
        const safeAddress = (d.address || '').toLowerCase();
        const safePlaceName = (d.place_name || '').toLowerCase();

        return keywords.every(kw => {
            const lowerKw = kw.toLowerCase();
            return safeAddress.includes(lowerKw) ||
                   safePlaceName.includes(lowerKw);
        });
    });

    const seenAddresses = new Set();
    const uniqueMatches = [];
    rawMatches.forEach(item => {
        const uniqueKey = item.address;
        if (!seenAddresses.has(uniqueKey)) {
            seenAddresses.add(uniqueKey);
            uniqueMatches.push(item);
        }
    });

    const finalResults = uniqueMatches.slice(0, 15);

    if (finalResults.length > 0) {
        searchResults.innerHTML = '';
        finalResults.forEach(item => {
            const div = document.createElement('div');
            div.className = 'search-item';
            div.innerHTML = `
                <span class="item-name">${escapeHtml(item.place_name)}</span>
                <span class="item-addr">${escapeHtml(item.address)}</span>
            `;
            div.addEventListener('click', () => moveToPoint(item.lat, item.lng));
            searchResults.appendChild(div);
        });
        searchResults.style.display = 'block';
    } else {
        searchResults.style.display = 'none';
    }
});

function moveToPoint(lat, lng) {
    setPeriod('60', document.getElementById('btn60'));
    toggleSearchMode(false);

    const latKey = parseFloat(lat).toFixed(6);
    const lngKey = parseFloat(lng).toFixed(6);
    const key = `${latKey}|${lngKey}`;

    const target = globalMarkers[key];

    if (target) {
        // 마커가 클러스터 내부에 있을 경우를 대비해 zoomToShowLayer 실행 후 openPopup 호출
        target.clusterGroup.zoomToShowLayer(target.marker, function() {
            target.marker.openPopup();
        });
    } else {
        // 필터링(예: 최근 60일) 조건에 의해 해당 위치에 생성된 마커가 없는 경우 지도 이동만 수행
        map.flyTo([lat, lng], 17, { duration: 1.5 });
    }
}

function parseDateString(dateStr) {
    if (!dateStr || dateStr.length !== 8) return null;
    const y = parseInt(dateStr.substring(0, 4));
    const m = parseInt(dateStr.substring(4, 6)) - 1;
    const d = parseInt(dateStr.substring(6, 8));
    return new Date(y, m, d);
}

function getDaysDiff(d1, d2) {
    const t1 = d1.getTime();
    const t2 = d2.getTime();
    return Math.floor((t1 - t2) / (24 * 60 * 60 * 1000));
}

function formatDate(str) {
    if(!str || str.length !== 8) return str;
    return `${str.substring(0,4)}.${str.substring(4,6)}.${str.substring(6,8)}`;
}

// 서울시/카카오 API에서 오는 값을 그대로 innerHTML에 넣으면 XSS 위험이 있어 이스케이프한다
function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str ?? '';
    return div.innerHTML;
}

function moveToCurrentLocation() {
    map.locate({setView: true, maxZoom: 14});
}

map.on('locationfound', e => {
    gpsLayer.clearLayers();
    L.circle(e.latlng, { radius: e.accuracy / 2, color: '#2ecc71', fillColor: '#2ecc71', fillOpacity: 0.1 }).addTo(gpsLayer);
    L.circleMarker(e.latlng, { radius: 8, color: '#fff', fillColor: '#2ecc71', fillOpacity: 1 }).addTo(gpsLayer);
});

Promise.all([loadGuBoundaries(), loadDongBoundaries(), loadData()]).then(() => {
    updateChoropleths();
    updateZoomLayers();
});
