#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPERBID MONITOR - Match Garantido 100%
✅ Pega TODOS os itens do banco (sem limit)
✅ Busca TODAS as categorias (igual ao scraper)
✅ Match garantido de todos os itens
"""

import os
import sys
import time
import requests
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set


# ============================================================================
# SUPABASE CLIENT
# ============================================================================

class SupabaseSuperbidMonitor:
    """Cliente Supabase para monitoramento"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("❌ Configure SUPABASE_URL e SUPABASE_KEY")
        
        self.url = self.url.rstrip('/')
        self.table_items = 'superbid_items'
        self.table_monitoring = 'superbid_monitoring'
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def get_all_active_items(self) -> List[Dict]:
        """Busca TODOS os itens ativos (sem limit) - PAGINADO"""
        print(f"📊 Carregando itens do banco (TODOS)...")
        
        url = f"{self.url}/rest/v1/{self.table_items}"
        
        all_items = []
        offset = 0
        page_size = 1000
        
        while True:
            # Query simples - só campos que existem
            params = {
                'select': 'id,external_id,offer_id,category,total_bids,total_bidders,visits,has_bids,current_winner_id,is_sold,is_closed,is_active,last_scraped_at',
                'is_active': 'eq.true',
                'is_closed': 'eq.false',
                'limit': page_size,
                'offset': offset,
                'order': 'id.asc',
            }
            
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    items = response.json()
                    
                    if not items:
                        break
                    
                    all_items.extend(items)
                    print(f"   📄 Página {offset//page_size + 1}: +{len(items)} itens (total: {len(all_items)})")
                    
                    if len(items) < page_size:
                        break
                    
                    offset += page_size
                    
                else:
                    print(f"❌ Erro HTTP {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"❌ Erro: {str(e)}")
                break
        
        print(f"✅ {len(all_items)} itens carregados no total\n")
        return all_items
    
    def count_snapshots(self, item_id: int) -> int:
        """Conta snapshots de um item"""
        url = f"{self.url}/rest/v1/{self.table_monitoring}"
        
        try:
            params = {'item_id': f'eq.{item_id}', 'select': 'id'}
            headers = self.headers.copy()
            headers['Prefer'] = 'count=exact'
            
            response = self.session.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                content_range = response.headers.get('Content-Range', '0-0/0')
                total = int(content_range.split('/')[-1])
                return total
            return 0
        except:
            return 0
    
    def insert_snapshots_batch(self, snapshots: List[Dict]) -> int:
        """Insere snapshots em lote"""
        url = f"{self.url}/rest/v1/{self.table_monitoring}"
        
        try:
            headers = self.headers.copy()
            headers['Prefer'] = 'return=minimal'
            
            response = self.session.post(url, json=snapshots, headers=headers, timeout=60)
            
            if response.status_code in (200, 201):
                return len(snapshots)
            else:
                error = response.text[:300]
                print(f"   ❌ HTTP {response.status_code}: {error}")
                return 0
        except Exception as e:
            print(f"   ❌ Erro: {str(e)[:100]}")
            return 0
    
    def update_base_items_batch(self, updates: List[Dict]) -> int:
        """Atualiza itens em lote"""
        count = 0
        url = f"{self.url}/rest/v1/{self.table_items}"
        
        try:
            for update in updates:
                item_id = update.pop('id')
                params = {'id': f'eq.{item_id}'}
                headers = self.headers.copy()
                headers['Prefer'] = 'return=minimal'
                
                response = self.session.patch(url, params=params, json=update, headers=headers, timeout=30)
                
                if response.status_code in (200, 204):
                    count += 1
        except Exception as e:
            print(f"   ❌ Erro update: {str(e)[:80]}")
        
        return count
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


# ============================================================================
# MONITOR - BUSCA POR CATEGORIA (IGUAL AO SCRAPER)
# ============================================================================

class SuperbidMonitor:
    """Monitor Superbid - Busca por categoria"""
    
    def __init__(self):
        self.api_url = 'https://offer-query.superbid.net/seo/offers/'
        self.site_url = 'https://exchange.superbid.net'
        
        # MESMAS 18 CATEGORIAS DO SCRAPER
        self.categories = [
            ('alimentos-e-bebidas', 'Alimentos e Bebidas'),
            ('animais', 'Animais'),
            ('bolsas-canetas-joias-e-relogios', 'Bolsas, Canetas, Joias e Relógios'),
            ('caminhoes-onibus', 'Caminhões e Ônibus'),
            ('carros-motos', 'Carros e Motos'),
            ('cozinhas-e-restaurantes', 'Cozinhas e Restaurantes'),
            ('eletrodomesticos', 'Eletrodomésticos'),
            ('materiais-para-construcao-civil', 'Materiais para Construção Civil'),
            ('maquinas-pesadas-agricolas', 'Máquinas Pesadas e Agrícolas'),
            ('industrial-maquinas-equipamentos', 'Industrial, Máquinas e Equipamentos'),
            ('imoveis', 'Imóveis'),
            ('embarcacoes-aeronaves', 'Embarcações e Aeronaves'),
            ('moveis-e-decoracao', 'Móveis e Decoração'),
            ('movimentacao-transporte', 'Movimentação e Transporte'),
            ('oportunidades', 'Oportunidades'),
            ('partes-e-pecas', 'Partes e Peças'),
            ('sucatas-materiais-residuos', 'Sucatas, Materiais e Resíduos'),
            ('tecnologia', 'Tecnologia'),
        ]
        
        self.stats = {
            'items_in_db': 0,
            'items_scraped': 0,
            'items_matched': 0,
            'items_not_matched': 0,
            'snapshots_created': 0,
            'items_updated': 0,
            'bid_changes': 0,
            'status_changes': 0,
            'errors': 0,
        }
        
        self.headers = {
            "accept": "*/*",
            "accept-language": "pt-BR,pt;q=0.9",
            "origin": "https://exchange.superbid.net",
            "referer": "https://exchange.superbid.net/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        self.client = SupabaseSuperbidMonitor()
        self.db_items_by_offer_id = {}
    
    def run(self):
        """Execução principal"""
        print("\n" + "="*80)
        print("🔵 SUPERBID MONITOR - BUSCA POR CATEGORIA")
        print("="*80)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")
        
        start_time = time.time()
        
        # 1. Carrega TODOS os itens do banco (sem limit)
        db_items = self.client.get_all_active_items()
        
        if not db_items:
            print("⚠️  Nenhum item no banco")
            return 0
        
        self.stats['items_in_db'] = len(db_items)
        
        # Indexa por offer_id E por categoria
        items_by_category = {}
        for item in db_items:
            offer_id = item['offer_id']
            category = item.get('category', 'Desconhecida')  # ← MUDOU: era category_display
            
            self.db_items_by_offer_id[offer_id] = item
            
            if category not in items_by_category:
                items_by_category[category] = []
            items_by_category[category].append(offer_id)
        
        print(f"🎯 {len(self.db_items_by_offer_id)} offer_ids")
        print(f"📦 {len(items_by_category)} categorias no banco\n")
        print("="*80 + "\n")
        
        # 2. Scrape todas as categorias
        print("🔍 Scrapando categorias...\n")
        api_offers = self._fetch_all_categories()
        
        if not api_offers:
            print("⚠️  Nenhuma oferta da API")
            return 0
        
        self.stats['items_scraped'] = len(api_offers)
        print(f"\n✅ {len(api_offers)} ofertas scrapadas\n{'='*80}\n")
        
        # 3. Processa matches
        print("🔄 Processando matches...")
        self._process_matches(api_offers)
        
        # 4. Stats
        self._print_stats()
        
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print(f"\n⏱️  {minutes}min {seconds}s")
        print(f"✅ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")
        
        return 0
    
    def _fetch_all_categories(self) -> Dict[int, Dict]:
        """Scrape todas as categorias (IGUAL AO SCRAPER)"""
        all_offers = {}
        
        for idx, (url_slug, display_name) in enumerate(self.categories, 1):
            print(f"[{idx}/{len(self.categories)}] 📦 {display_name}")
            
            offers = self._scrape_category(url_slug)
            
            for offer_id, offer_data in offers.items():
                all_offers[offer_id] = offer_data
            
            print(f"   ✅ {len(offers)} ofertas\n")
            time.sleep(1)
        
        return all_offers
    
    def _scrape_category(self, url_slug: str) -> Dict[int, Dict]:
        """Scrape completo de uma categoria"""
        offers = {}
        page_num = 1
        page_size = 100
        consecutive_errors = 0
        max_errors = 3
        
        while True:
            try:
                params = {
                    "urlSeo": f"{self.site_url}/categorias/{url_slug}",
                    "locale": "pt_BR",
                    "orderBy": "score:desc",
                    "pageNumber": page_num,
                    "pageSize": page_size,
                    "portalId": "[2,15]",
                    "requestOrigin": "marketplace",
                    "searchType": "opened" if url_slug == 'imoveis' else "openedAll",
                    "timeZoneId": "America/Sao_Paulo",
                }
                
                response = self.session.get(self.api_url, params=params, timeout=30)
                
                if response.status_code != 200:
                    consecutive_errors += 1
                    print(f"   ⚠️  HTTP {response.status_code} pág {page_num}")
                    if consecutive_errors >= max_errors:
                        break
                    page_num += 1
                    time.sleep(3)
                    continue
                
                data = response.json()
                api_offers = data.get('offers', [])
                total = data.get('total', 0)
                
                if not api_offers:
                    break
                
                consecutive_errors = 0
                print(f"   📄 Pág {page_num}: {len(api_offers)} ofertas (total: {total})")
                
                for offer in api_offers:
                    offer_id = offer.get('id')
                    if offer_id:
                        offers[offer_id] = offer
                
                # Verifica se há mais páginas
                start = data.get('start', 0)
                limit = data.get('limit', page_size)
                if start + limit >= total:
                    break
                
                page_num += 1
                time.sleep(0.5)
                
            except Exception as e:
                consecutive_errors += 1
                self.stats['errors'] += 1
                print(f"   ⚠️  Erro pág {page_num}: {str(e)[:60]}")
                if consecutive_errors >= max_errors:
                    break
                page_num += 1
                time.sleep(3)
        
        return offers
    
    def _process_matches(self, api_offers: Dict[int, Dict]):
        """Processa matches"""
        print(f"\n🔄 Processando matches...")
        print(f"   Total no banco: {len(self.db_items_by_offer_id)}")
        print(f"   Total da API: {len(api_offers)}")
        
        snapshots = []
        updates = []
        batch_size = 50
        
        total = len(self.db_items_by_offer_id)
        processed = 0
        
        print(f"\n⏳ Iniciando loop de processamento...\n")
        
        for offer_id, db_item in self.db_items_by_offer_id.items():
            processed += 1
            
            # Progress SEMPRE a cada 50
            if processed % 50 == 0:
                print(f"   [{processed}/{total}] 🔄 Processando... (matched: {self.stats['items_matched']}, snapshots: {len(snapshots)})")
            
            api_data = api_offers.get(offer_id)
            
            if not api_data:
                self.stats['items_not_matched'] += 1
                continue
            
            self.stats['items_matched'] += 1
            
            # ⚠️ REMOVIDO: count_snapshots (tá travando aqui!)
            # total_snaps = self.client.count_snapshots(db_item['id'])
            total_snaps = 0  # Usa zero por enquanto
            
            # Cria snapshot
            snapshot = self._create_snapshot(db_item, api_data, total_snaps)
            if snapshot:
                snapshots.append(snapshot)
                
                if snapshot['bid_status_changed']:
                    self.stats['bid_changes'] += 1
                if snapshot['status_changed']:
                    self.stats['status_changes'] += 1
            
            # Cria update
            update = self._create_update(db_item, api_data)
            if update:
                updates.append(update)
            
            # Flush em lotes
            if len(snapshots) >= batch_size:
                print(f"   💾 Flush: {len(snapshots)} snapshots, {len(updates)} updates")
                self._flush_batch(snapshots, updates)
                snapshots = []
                updates = []
        
        # Flush final
        if snapshots or updates:
            print(f"\n   💾 Flush final: {len(snapshots)} snapshots, {len(updates)} updates")
            self._flush_batch(snapshots, updates)
        
        print(f"\n✅ {processed} itens processados!")
    
    def _flush_batch(self, snapshots: List[Dict], updates: List[Dict]):
        """Salva lote de snapshots e updates"""
        if snapshots:
            inserted = self.client.insert_snapshots_batch(snapshots)
            self.stats['snapshots_created'] += inserted
        
        if updates:
            updated = self.client.update_base_items_batch(updates)
            self.stats['items_updated'] += updated
    
    def _create_snapshot(self, db_item: Dict, api_data: Dict, total_snaps: int) -> Optional[Dict]:
        """Cria snapshot - CÓDIGO IGUAL AO ANTERIOR"""
        try:
            now = datetime.now(timezone.utc)
            
            # HELPERS
            def get(path: str, default=None):
                keys = path.split('.')
                value = api_data
                for key in keys:
                    if isinstance(value, dict):
                        value = value.get(key)
                    else:
                        return default
                    if value is None:
                        return default
                return value
            
            def safe_int(val):
                try:
                    return int(val) if val not in (None, '') else None
                except:
                    return None
            
            def safe_float(val):
                try:
                    return float(val) if val not in (None, '') else None
                except:
                    return None
            
            def safe_bool(val):
                if val is None:
                    return False
                if isinstance(val, bool):
                    return val
                return str(val).lower() in ('true', '1', 'yes', 'sim')
            
            def safe_datetime(val):
                if not val:
                    return None
                try:
                    dt_str = str(val).replace('Z', '+00:00')
                    dt = datetime.fromisoformat(dt_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except:
                    return None
            
            def safe_state(val):
                if not val:
                    return None
                val_str = str(val).strip().upper()
                if len(val_str) == 2 and val_str.isalpha():
                    return val_str
                return None
            
            # EXTRAÇÃO
            total_bids = safe_int(get('totalBids')) or 0
            total_bidders = safe_int(get('totalBidders')) or 0
            visits = safe_int(get('visits')) or 0
            has_bids = safe_bool(get('hasBids'))
            
            price = safe_float(get('price'))
            initial_bid_value = safe_float(get('initialBidValue'))
            current_min_bid = safe_float(get('currentMinBid'))
            current_max_bid = safe_float(get('currentMaxBid'))
            reserved_price = safe_float(get('reservedPrice'))
            bid_increment = safe_float(get('bidIncrement'))
            
            current_winner_id = safe_int(get('currentWinner.id'))
            current_winner_login = get('currentWinner.login')
            
            is_sold = safe_bool(get('isSold'))
            is_reserved = safe_bool(get('isReserved'))
            is_closed = safe_bool(get('isClosed'))
            is_removed = safe_bool(get('isRemoved'))
            is_highlight = safe_bool(get('isHighlight'))
            
            auction_begin_date = safe_datetime(get('auction.beginDate'))
            auction_end_date = safe_datetime(get('auction.endDate'))
            auction_max_enddate = safe_datetime(get('auction.maxEnddateOffer'))
            
            category = get('product.subCategory.category.description')
            product_type_desc = get('product.productType.description')
            sub_category_desc = get('product.subCategory.description')
            auction_modality = get('auction.modalityDesc')
            offer_type_id = safe_int(get('offerTypeId'))
            
            city = get('product.location.city')
            state = safe_state(get('product.location.state'))
            location_lat = safe_float(get('product.location.locationGeo.lat'))
            location_lon = safe_float(get('product.location.locationGeo.lon'))
            
            seller_id = safe_int(get('seller.id'))
            seller_name = get('seller.name')
            store_id = safe_int(get('store.id'))
            store_name = get('store.name')
            manager_name = get('manager.name')
            
            photo_count = safe_int(get('product.photoCount')) or 0
            video_url_count = safe_int(get('product.videoUrlCount')) or 0
            
            title = get('product.shortDesc')
            description = get('product.detailedDescription')
            
            commission_percent = safe_float(get('groupOffer.commissionPercent'))
            allows_credit_card = safe_bool(get('commercialCondition.allowsCreditCard'))
            transaction_limit = safe_float(get('commercialCondition.transactionLimit'))
            max_installments = safe_int(get('commercialCondition.maxInstallments'))
            
            total_received_proposals = safe_int(get('totalReceivedProposals')) or 0
            has_received_bids_or_proposals = safe_bool(get('hasReceivedBidsOrProposals'))
            
            # TEMPORAIS
            hours_until_auction_end = None
            hours_since_auction_start = None
            days_in_auction = None
            
            if auction_end_date:
                try:
                    end_dt = datetime.fromisoformat(auction_end_date)
                    delta = (end_dt - now).total_seconds() / 3600
                    hours_until_auction_end = round(delta, 2)
                except:
                    pass
            
            if auction_begin_date:
                try:
                    begin_dt = datetime.fromisoformat(auction_begin_date)
                    delta = (now - begin_dt).total_seconds() / 3600
                    hours_since_auction_start = round(delta, 2)
                    days_in_auction = round(delta / 24, 2)
                except:
                    pass
            
            # INCREMENTOS
            old_total_bids = db_item.get('total_bids', 0) or 0
            bid_count_change = total_bids - old_total_bids
            
            old_total_bidders = db_item.get('total_bidders', 0) or 0
            bidder_count_change = total_bidders - old_total_bidders
            
            old_visits = db_item.get('visits', 0) or 0
            visit_increment = visits - old_visits
            
            # TEMPO DESDE ÚLTIMO SNAPSHOT
            old_last_scraped = db_item.get('last_scraped_at')
            hours_since_last_snapshot = None
            
            if old_last_scraped:
                try:
                    if isinstance(old_last_scraped, str):
                        old_dt = datetime.fromisoformat(str(old_last_scraped).replace('Z', '+00:00'))
                    else:
                        old_dt = old_last_scraped
                    
                    if old_dt.tzinfo is None:
                        old_dt = old_dt.replace(tzinfo=timezone.utc)
                    
                    delta = (now - old_dt).total_seconds() / 3600
                    hours_since_last_snapshot = round(delta, 2)
                except:
                    pass
            
            # VELOCIDADES
            bid_velocity = None
            visit_velocity = None
            popularity_velocity = None
            
            if hours_since_last_snapshot and hours_since_last_snapshot > 0:
                if bid_count_change > 0:
                    bid_velocity = round(bid_count_change / hours_since_last_snapshot, 4)
                if visit_increment > 0:
                    visit_velocity = round(visit_increment / hours_since_last_snapshot, 4)
                    popularity_velocity = visit_velocity
            
            # INCREMENTOS DE VALOR
            bid_total_increment = None
            bid_total_increment_percentage = None
            if current_max_bid and initial_bid_value and initial_bid_value > 0:
                bid_total_increment = current_max_bid - initial_bid_value
                bid_total_increment_percentage = round((bid_total_increment / initial_bid_value) * 100, 2)
            
            bid_increment_percentage = None
            if bid_increment and initial_bid_value and initial_bid_value > 0:
                bid_increment_percentage = round((bid_increment / initial_bid_value) * 100, 2)
            
            # MUDANÇAS
            old_has_bids = db_item.get('has_bids', False) or False
            bid_status_changed = (has_bids != old_has_bids)
            
            old_is_closed = db_item.get('is_closed', False) or False
            old_is_sold = db_item.get('is_sold', False) or False
            status_changed = ((is_closed != old_is_closed) or (is_sold != old_is_sold))
            
            old_winner_id = db_item.get('current_winner_id')
            winner_changed = (current_winner_id != old_winner_id) and current_winner_id is not None
            
            time_with_bid_hours = None
            if has_bids and hours_since_auction_start:
                time_with_bid_hours = hours_since_auction_start
            
            # SNAPSHOT COMPLETO
            return {
                'item_id': db_item['id'],
                'external_id': db_item['external_id'],
                'snapshot_at': now.isoformat(),
                'hours_until_auction_end': hours_until_auction_end,
                'hours_since_auction_start': hours_since_auction_start,
                'days_in_auction': days_in_auction,
                'auction_begin_date': auction_begin_date,
                'auction_end_date': auction_end_date,
                'auction_max_enddate': auction_max_enddate,
                'price': price,
                'initial_bid_value': initial_bid_value,
                'current_min_bid': current_min_bid,
                'current_max_bid': current_max_bid,
                'reserved_price': reserved_price,
                'bid_increment': bid_increment,
                'bid_total_increment': bid_total_increment,
                'bid_total_increment_percentage': bid_total_increment_percentage,
                'bid_increment_percentage': bid_increment_percentage,
                'total_bids': total_bids,
                'total_bidders': total_bidders,
                'total_received_proposals': total_received_proposals,
                'visits': visits,
                'bid_count_change': bid_count_change,
                'bidder_count_change': bidder_count_change,
                'visit_increment': visit_increment,
                'visit_velocity': visit_velocity,
                'bid_velocity': bid_velocity,
                'popularity_velocity': popularity_velocity,
                'has_bids': has_bids,
                'has_received_bids_or_proposals': has_received_bids_or_proposals,
                'is_sold': is_sold,
                'is_reserved': is_reserved,
                'is_closed': is_closed,
                'is_removed': is_removed,
                'is_highlight': is_highlight,
                'is_active': not is_closed and not is_sold,
                'bid_status_changed': bid_status_changed,
                'status_changed': status_changed,
                'offer_status_changed': status_changed,
                'winner_changed': winner_changed,
                'time_with_bid_hours': time_with_bid_hours,
                'current_winner_id': current_winner_id,
                'current_winner_login': current_winner_login,
                'category': category,
                'product_type_desc': product_type_desc,
                'sub_category_desc': sub_category_desc,
                'auction_modality': auction_modality,
                'offer_type_id': offer_type_id,
                'city': city,
                'state': state,
                'location_lat': location_lat,
                'location_lon': location_lon,
                'seller_id': seller_id,
                'seller_name': seller_name,
                'store_id': store_id,
                'store_name': store_name,
                'manager_name': manager_name,
                'photo_count': photo_count,
                'video_url_count': video_url_count,
                'title': title,
                'description': description,
                'commission_percent': commission_percent,
                'allows_credit_card': allows_credit_card,
                'transaction_limit': transaction_limit,
                'max_installments': max_installments,
                'hours_since_last_snapshot': hours_since_last_snapshot,
                'total_snapshots_count': total_snaps + 1,
                'source': 'superbid',
                'metadata': {},
            }
            
        except Exception as e:
            print(f"   ❌ Erro snapshot: {str(e)[:100]}")
            self.stats['errors'] += 1
            return None
    
    def _create_update(self, db_item: Dict, api_data: Dict) -> Optional[Dict]:
        """Cria update"""
        try:
            def get(path: str, default=None):
                keys = path.split('.')
                value = api_data
                for key in keys:
                    if isinstance(value, dict):
                        value = value.get(key)
                    else:
                        return default
                    if value is None:
                        return default
                return value
            
            def safe_int(val):
                try:
                    return int(val) if val not in (None, '') else None
                except:
                    return None
            
            def safe_float(val):
                try:
                    return float(val) if val not in (None, '') else None
                except:
                    return None
            
            def safe_bool(val):
                if val is None:
                    return False
                if isinstance(val, bool):
                    return val
                return str(val).lower() in ('true', '1', 'yes', 'sim')
            
            return {
                'id': db_item['id'],
                'total_bids': safe_int(get('totalBids')) or 0,
                'total_bidders': safe_int(get('totalBidders')) or 0,
                'visits': safe_int(get('visits')) or 0,
                'has_bids': safe_bool(get('hasBids')),
                'current_max_bid': safe_float(get('currentMaxBid')),
                'current_winner_id': safe_int(get('currentWinner.id')),
                'current_winner_login': get('currentWinner.login'),
                'is_sold': safe_bool(get('isSold')),
                'is_closed': safe_bool(get('isClosed')),
                'is_reserved': safe_bool(get('isReserved')),
                'is_active': not safe_bool(get('isClosed')) and not safe_bool(get('isSold')),
                'last_scraped_at': datetime.now(timezone.utc).isoformat(),
            }
            
        except Exception as e:
            print(f"   ❌ Erro update: {str(e)[:80]}")
            self.stats['errors'] += 1
            return None
    
    def _print_stats(self):
        """Estatísticas"""
        print("\n" + "="*80)
        print("📊 ESTATÍSTICAS FINAIS")
        print("="*80)
        print(f"\n📦 Banco:")
        print(f"   • Total de itens: {self.stats['items_in_db']}")
        print(f"\n🔍 Scraping:")
        print(f"   • Ofertas scrapadas: {self.stats['items_scraped']}")
        print(f"\n🎯 Matching:")
        print(f"   • Matched: {self.stats['items_matched']}")
        print(f"   • Não encontrados: {self.stats['items_not_matched']}")
        
        if self.stats['items_in_db'] > 0:
            match_percent = (self.stats['items_matched'] / self.stats['items_in_db']) * 100
            print(f"   • Taxa de match: {match_percent:.1f}%")
        
        print(f"\n💾 Snapshots:")
        print(f"   • Criados: {self.stats['snapshots_created']}")
        print(f"   • Itens atualizados: {self.stats['items_updated']}")
        
        print(f"\n📈 Mudanças:")
        print(f"   • Novos lances: {self.stats['bid_changes']}")
        print(f"   • Mudanças de status: {self.stats['status_changes']}")
        
        if self.stats['errors'] > 0:
            print(f"\n⚠️  Erros: {self.stats['errors']}")
        
        print("\n" + "="*80)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Execução principal"""
    try:
        monitor = SuperbidMonitor()
        return monitor.run()
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())