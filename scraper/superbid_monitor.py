#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPERBID MONITOR - Monitoramento Temporal para ML
✅ Busca itens ativos da base
✅ Atualiza dados via API
✅ Calcula features temporais e mudanças
✅ Cria snapshots históricos em superbid_monitoring
✅ Atualiza superbid_items
"""

import os
import sys
import time
import requests
from datetime import datetime, timezone
from typing import List, Dict, Optional


# ============================================================================
# SUPABASE CLIENT - MONITORING
# ============================================================================

class SupabaseSuperbidMonitor:
    """Cliente Supabase para monitoramento de ofertas Superbid"""
    
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
    
    def get_active_items(self, limit: int = 500) -> List[Dict]:
        """
        Busca itens ativos para monitorar
        Critérios:
        - is_active = true
        - is_closed = false
        - auction_end_date > now (ainda não terminou)
        """
        print(f"📊 Buscando itens ativos (limit={limit})...")
        
        # Data/hora atual
        now = datetime.utcnow().isoformat()
        
        # Query com filtros
        url = f"{self.url}/rest/v1/{self.table_items}"
        
        params = {
            'select': '*',
            'is_active': 'eq.true',
            'is_closed': 'eq.false',
            'auction_end_date': f'gt.{now}',
            'order': 'auction_end_date.asc',  # Prioriza os que vão acabar primeiro
            'limit': limit,
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                items = response.json()
                print(f"✅ {len(items)} itens ativos encontrados")
                return items
            else:
                error_msg = response.text[:200] if response.text else 'Sem detalhes'
                print(f"❌ Erro ao buscar itens: HTTP {response.status_code}")
                print(f"   {error_msg}")
                return []
                
        except Exception as e:
            print(f"❌ Erro ao buscar itens: {str(e)}")
            return []
    
    def insert_snapshot(self, snapshot: Dict) -> bool:
        """Insere snapshot na tabela de monitoramento"""
        url = f"{self.url}/rest/v1/{self.table_monitoring}"
        
        try:
            # Headers específicos para insert
            headers = self.headers.copy()
            headers['Prefer'] = 'return=minimal'
            
            response = self.session.post(
                url, 
                json=snapshot,
                headers=headers,
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return True
            else:
                error_msg = response.text[:200] if response.text else 'Sem detalhes'
                print(f"   ⚠️  Erro ao inserir snapshot: HTTP {response.status_code}")
                print(f"      {error_msg}")
                return False
                
        except Exception as e:
            print(f"   ⚠️  Erro ao inserir snapshot: {str(e)[:100]}")
            return False
    
    def update_base_item(self, item_id: int, update_data: Dict) -> bool:
        """Atualiza item na tabela base superbid_items"""
        url = f"{self.url}/rest/v1/{self.table_items}"
        
        try:
            params = {'id': f'eq.{item_id}'}
            
            # Headers específicos para update
            headers = self.headers.copy()
            headers['Prefer'] = 'return=minimal'
            
            response = self.session.patch(
                url,
                params=params,
                json=update_data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code in (200, 204):
                return True
            else:
                error_msg = response.text[:200] if response.text else 'Sem detalhes'
                print(f"   ⚠️  Erro ao atualizar item: HTTP {response.status_code}")
                print(f"      {error_msg}")
                return False
                
        except Exception as e:
            print(f"   ⚠️  Erro ao atualizar item: {str(e)[:100]}")
            return False
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


# ============================================================================
# MONITOR PRINCIPAL
# ============================================================================

class SuperbidMonitor:
    """Monitor de ofertas Superbid para ML e detecção de oportunidades"""
    
    def __init__(self):
        self.source = 'superbid'
        self.api_url = 'https://offer-query.superbid.net/offer/'
        
        self.stats = {
            'total_monitored': 0,
            'updated': 0,
            'new_bids': 0,
            'status_changes': 0,
            'errors': 0,
            'snapshots_created': 0,
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
        
        # Cliente Supabase para monitoring
        self.client = SupabaseSuperbidMonitor()
    
    def run(self):
        """Execução principal do monitor"""
        print("\n" + "="*80)
        print("🔵 SUPERBID MONITOR - ML & OPORTUNIDADES")
        print("="*80)
        print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")
        
        start_time = time.time()
        
        # ========================================
        # ETAPA 1: BUSCAR ITENS ATIVOS
        # ========================================
        print("📊 Buscando itens ativos para monitorar...")
        active_items = self.client.get_active_items()
        
        if not active_items:
            print("⚠️  Nenhum item ativo encontrado")
            return 0
        
        print(f"✅ {len(active_items)} itens ativos encontrados")
        print("="*80 + "\n")
        
        # ========================================
        # ETAPA 2: PROCESSAR CADA ITEM
        # ========================================
        for idx, item in enumerate(active_items, 1):
            print(f"[{idx}/{len(active_items)}] 📦 {item.get('title', 'Sem título')[:60]}")
            print(f"{'─'*80}")
            
            try:
                self._process_item(item)
                self.stats['total_monitored'] += 1
                
            except Exception as e:
                self.stats['errors'] += 1
                print(f"   ❌ Erro: {str(e)[:100]}")
            
            # Rate limiting
            if idx < len(active_items):
                time.sleep(1.5)
            
            print()
        
        # ========================================
        # ETAPA 3: ESTATÍSTICAS
        # ========================================
        self._print_stats()
        
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print(f"\n⏱️  Duração: {minutes}min {seconds}s")
        print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")
        
        return 0
    
    def _process_item(self, old_item: Dict):
        """Processa um item: busca dados atuais, calcula features, cria snapshot"""
        offer_id = old_item.get('offer_id')
        if not offer_id:
            return
        
        # ========================================
        # 1. BUSCAR DADOS ATUALIZADOS DA API
        # ========================================
        new_data = self._fetch_offer(offer_id)
        if not new_data:
            print(f"   ⚠️  Não foi possível atualizar dados")
            return
        
        # ========================================
        # 2. CALCULAR FEATURES TEMPORAIS
        # ========================================
        snapshot = self._calculate_features(old_item, new_data)
        
        # ========================================
        # 3. CRIAR SNAPSHOT NA TABELA MONITORING
        # ========================================
        try:
            self.client.insert_snapshot(snapshot)
            self.stats['snapshots_created'] += 1
            print(f"   ✅ Snapshot criado")
        except Exception as e:
            print(f"   ⚠️  Erro ao criar snapshot: {str(e)[:80]}")
        
        # ========================================
        # 4. ATUALIZAR ITEM NA TABELA BASE
        # ========================================
        if snapshot.get('bid_status_changed') or snapshot.get('status_changed'):
            try:
                update_data = self._prepare_update(new_data)
                self.client.update_base_item(old_item['id'], update_data)
                self.stats['updated'] += 1
                print(f"   🔄 Item atualizado na base")
                
                if snapshot.get('bid_status_changed'):
                    self.stats['new_bids'] += 1
                if snapshot.get('status_changed'):
                    self.stats['status_changes'] += 1
                    
            except Exception as e:
                print(f"   ⚠️  Erro ao atualizar base: {str(e)[:80]}")
    
    def _fetch_offer(self, offer_id: int) -> Optional[Dict]:
        """Busca dados atualizados de uma oferta na API"""
        try:
            params = {
                "locale": "pt_BR",
                "offerId": offer_id,
                "portalId": "[2,15]",
                "requestOrigin": "marketplace",
                "timeZoneId": "America/Sao_Paulo",
            }
            
            response = self.session.get(
                self.api_url,
                params=params,
                timeout=15
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"   ⚠️  HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f"   ⚠️  Erro na API: {str(e)[:60]}")
            return None
    
    def _calculate_features(self, old_item: Dict, new_data: Dict) -> Dict:
        """Calcula features temporais e detecta mudanças"""
        now = datetime.now(timezone.utc)
        
        # ========================================
        # HELPERS
        # ========================================
        def get(path: str, default=None):
            """Extrai valor usando dot notation"""
            keys = path.split('.')
            value = new_data
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return default
                if value is None:
                    return default
            return value
        
        def safe_int(val):
            if val is None or val == '':
                return None
            try:
                return int(val)
            except:
                return None
        
        def safe_float(val):
            if val is None or val == '':
                return None
            try:
                return float(val)
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
                return datetime.fromisoformat(dt_str)
            except:
                return None
        
        # ========================================
        # EXTRAÇÃO DE DADOS NOVOS
        # ========================================
        
        # IDs
        item_id = old_item['id']
        external_id = old_item['external_id']
        offer_id = safe_int(get('id'))
        
        # Lances e atividade
        total_bids = safe_int(get('totalBids')) or 0
        total_bidders = safe_int(get('totalBidders')) or 0
        visits = safe_int(get('visits')) or 0
        has_bids = safe_bool(get('hasBids'))
        
        # Valores
        price = safe_float(get('price'))
        initial_bid_value = safe_float(get('initialBidValue'))
        current_min_bid = safe_float(get('currentMinBid'))
        current_max_bid = safe_float(get('currentMaxBid'))
        reserved_price = safe_float(get('reservedPrice'))
        bid_increment = safe_float(get('bidIncrement'))
        
        # Vencedor
        current_winner_id = safe_int(get('currentWinner.id'))
        current_winner_login = get('currentWinner.login')
        
        # Status
        is_sold = safe_bool(get('isSold'))
        is_reserved = safe_bool(get('isReserved'))
        is_closed = safe_bool(get('isClosed'))
        is_removed = safe_bool(get('isRemoved'))
        is_highlight = safe_bool(get('isHighlight'))
        
        # Datas
        auction_begin_date = safe_datetime(get('auction.beginDate'))
        auction_end_date = safe_datetime(get('auction.endDate'))
        auction_max_enddate = safe_datetime(get('auction.maxEnddateOffer'))
        
        # Categoria
        category = get('product.subCategory.category.description')
        product_type_desc = get('product.productType.description')
        sub_category_desc = get('product.subCategory.description')
        
        # Localização
        city = get('product.location.city')
        state = get('product.location.state')
        location_lat = safe_float(get('product.location.locationGeo.lat'))
        location_lon = safe_float(get('product.location.locationGeo.lon'))
        
        # Vendedor
        seller_id = safe_int(get('seller.id'))
        seller_name = get('seller.name')
        store_id = safe_int(get('store.id'))
        store_name = get('store.name')
        manager_name = get('manager.name')
        
        # Mídia
        photo_count = safe_int(get('product.photoCount')) or 0
        video_url_count = safe_int(get('product.videoUrlCount')) or 0
        
        # Textos para NLP
        title = get('product.shortDesc', 'Sem Título')
        description = get('product.detailedDescription')
        
        # ========================================
        # CÁLCULO DE FEATURES TEMPORAIS
        # ========================================
        
        # Tempo até fim do leilão
        hours_until_auction_end = None
        if auction_end_date:
            delta = (auction_end_date - now).total_seconds() / 3600
            hours_until_auction_end = round(delta, 2)
        
        # Tempo desde início do leilão
        hours_since_auction_start = None
        days_in_auction = None
        if auction_begin_date:
            delta = (now - auction_begin_date).total_seconds() / 3600
            hours_since_auction_start = round(delta, 2)
            days_in_auction = round(delta / 24, 2)
        
        # ========================================
        # CÁLCULO DE INCREMENTOS E VELOCIDADES
        # ========================================
        
        # Incrementos de lances
        old_total_bids = old_item.get('total_bids', 0) or 0
        bid_count_change = total_bids - old_total_bids
        
        old_total_bidders = old_item.get('total_bidders', 0) or 0
        bidder_count_change = total_bidders - old_total_bidders
        
        # Incremento de visitas
        old_visits = old_item.get('visits', 0) or 0
        visit_increment = visits - old_visits
        
        # Tempo desde último snapshot
        old_last_scraped = old_item.get('last_scraped_at')
        hours_since_last_snapshot = None
        if old_last_scraped:
            try:
                old_dt = datetime.fromisoformat(str(old_last_scraped).replace('Z', '+00:00'))
                delta = (now - old_dt).total_seconds() / 3600
                hours_since_last_snapshot = round(delta, 2)
            except:
                pass
        
        # Velocidades (por hora)
        bid_velocity = None
        visit_velocity = None
        popularity_velocity = None
        
        if hours_since_last_snapshot and hours_since_last_snapshot > 0:
            if bid_count_change > 0:
                bid_velocity = round(bid_count_change / hours_since_last_snapshot, 4)
            
            if visit_increment > 0:
                visit_velocity = round(visit_increment / hours_since_last_snapshot, 4)
                popularity_velocity = visit_velocity
        
        # Incremento total de valor
        bid_total_increment = None
        bid_total_increment_percentage = None
        if current_max_bid and initial_bid_value and initial_bid_value > 0:
            bid_total_increment = current_max_bid - initial_bid_value
            bid_total_increment_percentage = round(
                (bid_total_increment / initial_bid_value) * 100, 2
            )
        
        # Incremento percentual do lance
        bid_increment_percentage = None
        if bid_increment and initial_bid_value and initial_bid_value > 0:
            bid_increment_percentage = round(
                (bid_increment / initial_bid_value) * 100, 2
            )
        
        # ========================================
        # DETECÇÃO DE MUDANÇAS
        # ========================================
        
        # Mudança de status de lance
        old_has_bids = old_item.get('has_bids', False) or False
        bid_status_changed = (has_bids != old_has_bids)
        
        # Mudança de status geral
        old_is_closed = old_item.get('is_closed', False) or False
        old_is_sold = old_item.get('is_sold', False) or False
        status_changed = (
            (is_closed != old_is_closed) or 
            (is_sold != old_is_sold)
        )
        
        # Mudança de vencedor
        old_winner_id = old_item.get('current_winner_id')
        winner_changed = (current_winner_id != old_winner_id) and current_winner_id is not None
        
        # Tempo com lance
        time_with_bid_hours = None
        if has_bids and hours_since_auction_start:
            time_with_bid_hours = hours_since_auction_start
        
        # ========================================
        # COMISSÃO E PAGAMENTO
        # ========================================
        commission_percent = safe_float(get('groupOffer.commissionPercent'))
        allows_credit_card = safe_bool(get('commercialCondition.allowsCreditCard'))
        transaction_limit = safe_float(get('commercialCondition.transactionLimit'))
        max_installments = safe_int(get('commercialCondition.maxInstallments'))
        
        # ========================================
        # RETORNO DO SNAPSHOT
        # ========================================
        return {
            # IDs
            'item_id': item_id,
            'external_id': external_id,
            'snapshot_at': now.isoformat(),
            
            # Temporal
            'hours_until_auction_end': hours_until_auction_end,
            'hours_since_auction_start': hours_since_auction_start,
            'days_in_auction': days_in_auction,
            'auction_begin_date': auction_begin_date.isoformat() if auction_begin_date else None,
            'auction_end_date': auction_end_date.isoformat() if auction_end_date else None,
            'auction_max_enddate': auction_max_enddate.isoformat() if auction_max_enddate else None,
            
            # Valores
            'price': price,
            'initial_bid_value': initial_bid_value,
            'current_min_bid': current_min_bid,
            'current_max_bid': current_max_bid,
            'reserved_price': reserved_price,
            'bid_increment': bid_increment,
            'bid_total_increment': bid_total_increment,
            'bid_total_increment_percentage': bid_total_increment_percentage,
            'bid_increment_percentage': bid_increment_percentage,
            
            # Atividade
            'total_bids': total_bids,
            'total_bidders': total_bidders,
            'total_received_proposals': safe_int(get('totalReceivedProposals')) or 0,
            'visits': visits,
            
            # Incrementos
            'bid_count_change': bid_count_change,
            'bidder_count_change': bidder_count_change,
            'visit_increment': visit_increment,
            
            # Velocidades
            'visit_velocity': visit_velocity,
            'bid_velocity': bid_velocity,
            'popularity_velocity': popularity_velocity,
            
            # Estados
            'has_bids': has_bids,
            'has_received_bids_or_proposals': safe_bool(get('hasReceivedBidsOrProposals')),
            'is_sold': is_sold,
            'is_reserved': is_reserved,
            'is_closed': is_closed,
            'is_removed': is_removed,
            'is_highlight': is_highlight,
            'is_active': not is_closed and not is_sold,
            
            # Mudanças
            'bid_status_changed': bid_status_changed,
            'status_changed': status_changed,
            'offer_status_changed': status_changed,
            'winner_changed': winner_changed,
            'time_with_bid_hours': time_with_bid_hours,
            
            # Vencedor
            'current_winner_id': current_winner_id,
            'current_winner_login': current_winner_login,
            
            # Categoria
            'category': category,
            'product_type_desc': product_type_desc,
            'sub_category_desc': sub_category_desc,
            'auction_modality': get('auction.modalityDesc'),
            'offer_type_id': safe_int(get('offerTypeId')),
            
            # Localização
            'city': city,
            'state': state,
            'location_lat': location_lat,
            'location_lon': location_lon,
            
            # Vendedor
            'seller_id': seller_id,
            'seller_name': seller_name,
            'store_id': store_id,
            'store_name': store_name,
            'manager_name': manager_name,
            
            # Mídia
            'photo_count': photo_count,
            'video_url_count': video_url_count,
            
            # NLP
            'title': title,
            'description': description,
            
            # Pagamento
            'commission_percent': commission_percent,
            'allows_credit_card': allows_credit_card,
            'transaction_limit': transaction_limit,
            'max_installments': max_installments,
            
            # Rastreamento
            'hours_since_last_snapshot': hours_since_last_snapshot,
            'total_snapshots_count': 1,  # Será incrementado no banco
            
            # Metadata
            'source': 'superbid',
            'metadata': {},
        }
    
    def _prepare_update(self, new_data: Dict) -> Dict:
        """Prepara dados para atualizar item na tabela base"""
        def get(path: str, default=None):
            keys = path.split('.')
            value = new_data
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return default
                if value is None:
                    return default
            return value
        
        def safe_int(val):
            if val is None or val == '':
                return None
            try:
                return int(val)
            except:
                return None
        
        def safe_float(val):
            if val is None or val == '':
                return None
            try:
                return float(val)
            except:
                return None
        
        def safe_bool(val):
            if val is None:
                return False
            if isinstance(val, bool):
                return val
            return str(val).lower() in ('true', '1', 'yes', 'sim')
        
        return {
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
            'last_scraped_at': datetime.now().isoformat(),
        }
    
    def _print_stats(self):
        """Imprime estatísticas finais"""
        print("\n" + "="*80)
        print("📊 ESTATÍSTICAS DO MONITORAMENTO")
        print("="*80)
        print(f"   • Total monitorado: {self.stats['total_monitored']}")
        print(f"   • Snapshots criados: {self.stats['snapshots_created']}")
        print(f"   • Itens atualizados: {self.stats['updated']}")
        print(f"   • Novos lances: {self.stats['new_bids']}")
        print(f"   • Mudanças de status: {self.stats['status_changes']}")
        print(f"   • Erros: {self.stats['errors']}")
        print("="*80)


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