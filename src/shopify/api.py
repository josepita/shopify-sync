import requests
import logging
from typing import Dict, Any, Optional, List
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class ShopifyAPI:
    def __init__(self, shop_url: str, access_token: str, api_version: str = "2024-10"):
        """
        Inicializa la conexión con Shopify GraphQL Admin API
        """
        self.shop_url = shop_url.rstrip('/')
        self.access_token = access_token
        self.api_version = api_version
        self.endpoint = f"https://{self.shop_url}/admin/api/{self.api_version}/graphql.json"
        self.headers = {
            'X-Shopify-Access-Token': self.access_token,
            'Content-Type': 'application/json'
        }
        self.last_request_time = 0
        self.min_request_interval = 0.5

    def _handle_rate_limit(self):
        """Maneja el rate limiting para no exceder los límites de la API"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        self.last_request_time = time.time()

    def _make_request(self, query: str, variables: Dict = None) -> Dict:
        """
        Realiza una petición GraphQL a Shopify
        """
        self._handle_rate_limit()
        try:
            response = requests.post(
                self.endpoint,
                headers=self.headers,
                json={'query': query, 'variables': variables or {}}
            )
            
            response.raise_for_status()
            data = response.json()
            
            if 'errors' in data:
                errors = data['errors']
                logger.error(f"GraphQL errors: {errors}")
                raise Exception(errors[0]['message'])
                
            return data.get('data', {})
            
        except Exception as e:
            logger.error(f"Error en petición GraphQL: {str(e)}")
            raise

    def get_product(self, product_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene la información detallada de un producto
        """
        query = """
        query getProduct($id: ID!) {
          product(id: $id) {
            id
            title
            handle
            status
            variants(first: 50) {
              edges {
                node {
                  id
                  title
                  sku
                  price
                  compareAtPrice
                  inventoryItem {
                    id
                    tracked
                    inventoryLevels(first: 1) {
                      edges {
                        node {
                          id
                          location {
                            id
                            name
                          }
                          quantities(names: ["available", "committed", "incoming", "on_hand", "reserved"]) {
                            name
                            quantity
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        try:
            variables = {'id': f'gid://shopify/Product/{product_id}'}
            result = self._make_request(query, variables)
            return result.get('product')
        except Exception as e:
            logger.error(f"Error obteniendo producto {product_id}: {str(e)}")
            return None

    def update_variant_price(self, product_id: str, variant_id: str, cost: float, margin: float) -> bool:
        query = """
        mutation bulkUpdateVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
        productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants {
            id
            price
            inventoryItem {
                unitCost {
                amount
                }
            }
            }
            userErrors {
            field
            message
            }
        }
        }
        """
        
        try:
            calculated_price = round(cost * margin, 2)
            variables = {
                'productId': f'gid://shopify/Product/{product_id}',
                'variants': [{
                    'id': f'gid://shopify/ProductVariant/{variant_id}',
                    'price': str(calculated_price),
                    'inventoryItem': {
                        'cost': cost
                    }
                }]
            }
            
            result = self._make_request(query, variables)
            user_errors = result.get('productVariantsBulkUpdate', {}).get('userErrors', [])
            
            if user_errors:
                logger.error(f"Errores actualizando precio/coste: {user_errors}")
                return False
                
            logger.info(f"Precio actualizado a {calculated_price} y coste a {cost} para variante {variant_id}")
            return True
        except Exception as e:
            logger.error(f"Error actualizando precio/coste de variante {variant_id}: {str(e)}")
            return False


    def get_inventory_level(self, inventory_item_id: str, location_id: str) -> Optional[int]:
        """
        Obtiene el nivel actual de inventario para un item específico
        """
        query = """
        query inventoryItemToProductVariant($inventoryItemId: ID!) {
          inventoryItem(id: $inventoryItemId) {
            id
            inventoryLevels(first: 1) {
              edges {
                node {
                  id
                  location {
                    id
                    name
                  }
                  quantities(names: ["available", "committed", "incoming", "on_hand", "reserved"]) {
                    name
                    quantity
                  }
                }
              }
            }
            variant {
              id
              title
              product {
                id
                title
              }
            }
          }
        }
        """
        
        try:
            variables = {
                'inventoryItemId': f'gid://shopify/InventoryItem/{inventory_item_id}'
            }
            
            result = self._make_request(query, variables)
            inventory_levels = result.get('inventoryItem', {}).get('inventoryLevels', {}).get('edges', [])
            
            for edge in inventory_levels:
                node = edge['node']
                if node['location']['id'].endswith(location_id):
                    quantities = node.get('quantities', [])
                    for quantity_entry in quantities:
                        if quantity_entry['name'] == "available":
                            return quantity_entry['quantity']

            logger.warning(f"No se encontró inventario para la ubicación {location_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error obteniendo nivel de inventario para item {inventory_item_id}: {str(e)}")
            return None

    def update_inventory_quantity(self, inventory_item_id: str, location_id: str, desired_quantity: int) -> bool:
        """
        Actualiza el inventario usando la mutación inventorySetQuantities
        """
        query = """
        mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
        inventorySetQuantities(input: $input) {
            inventoryAdjustmentGroup {
            createdAt
            reason
            changes {
                delta
            }
            }
            userErrors {
            field
            message
            }
        }
        }
        """
        
        try:
            # Log para debug de la cantidad
            logger.debug(f"Valor recibido para desired_quantity: {desired_quantity}")

            variables = {
                'input': {
                    'name': "available",  # Tipo de ajuste: 'available' o 'on_hand'
                    'quantities': [
                        {
                            'inventoryItemId': f'gid://shopify/InventoryItem/{inventory_item_id}',  # ID del item
                            'locationId': f'gid://shopify/Location/{location_id}',  # ID de la ubicación
                            'quantity': desired_quantity  # Nueva cantidad
                        }
                    ],
                    'reason': "restock",  # Motivo del ajuste
                    'ignoreCompareQuantity': True  # Ignorar comparación si no usas compareQuantity
                }
            }

            logger.info(f"Actualizando inventario: Inventory Item ID: {inventory_item_id}, Location ID: {location_id}, Nueva cantidad: {desired_quantity}")
            
            # Realiza la petición GraphQL
            result = self._make_request(query, variables)
            
            # Verifica si hubo errores
            user_errors = result.get('inventorySetQuantities', {}).get('userErrors', [])
            if user_errors:
                logger.error(f"Errores ajustando inventario: {user_errors}")
                return False

            logger.info("Inventario actualizado correctamente.")
            return True
            
        except Exception as e:
            logger.error(f"Error ajustando inventario: {str(e)}")
            return False


    def bulk_price_update(self, variant_updates: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Actualiza precios de múltiples variantes en una sola operación
        """
        query = """
        mutation bulkUpdateVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants {
              id
              price
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        
        try:
            product_id = variant_updates[0]['product_id'] if variant_updates else None
            if not product_id:
                raise ValueError("Se requiere un product_id para realizar la actualización masiva.")

            variants_data = [
                {
                    'id': f'gid://shopify/ProductVariant/{update["variant_id"]}',
                    'price': str(update['price'])
                }
                for update in variant_updates
            ]
            
            variables = {
                'productId': f'gid://shopify/Product/{product_id}',
                'variants': variants_data
            }
            
            result = self._make_request(query, variables)
            user_errors = result.get('productVariantsBulkUpdate', {}).get('userErrors', [])
            
            if user_errors:
                logger.error(f"Errores en actualización masiva: {user_errors}")
                return {update['variant_id']: False for update in variant_updates}
            
            updated_variants = result.get('productVariantsBulkUpdate', {}).get('productVariants', [])
            results = {}
            
            for update in variant_updates:
                variant_id = update['variant_id']
                success = any(v['id'].endswith(variant_id) for v in updated_variants)
                results[variant_id] = success
            
            return results
            
        except Exception as e:
            logger.error(f"Error en actualización masiva de precios: {str(e)}")
            return {update['variant_id']: False for update in variant_updates}
