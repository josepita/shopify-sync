import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.shopify.api import ShopifyAPI
from dotenv import load_dotenv
import logging
from pprint import pprint

logging.basicConfig(level=logging.INFO)
load_dotenv()

def test_shopify_api():
    # Crear instancia de API
    shopify = ShopifyAPI(
        shop_url=os.getenv('SHOPIFY_SHOP_URL'),
        access_token=os.getenv('SHOPIFY_ACCESS_TOKEN')
    )
    
    print("\nPruebas de API Shopify")
    print("-" * 50)
    
    # Obtener producto de ejemplo
    product_id = input("\nIntroduce el ID de un producto de tu tienda: ")
    
    try:
        # 1. Probar obtención de producto
        print("\n1. Obteniendo información del producto...")
        product = shopify.get_product(product_id)
        
        if product:
            print("\n✅ Producto obtenido:")
            print(f"Título: {product['title']}")
            
            # Obtener primera variante
            if product.get('variants', {}).get('edges'):
                variant = product['variants']['edges'][0]['node']
                variant_id = variant['id'].split('/')[-1]  # Extraer ID numérico
                current_price = float(variant['price'])
                
                # 2. Probar actualización de precio y costo
                print(f"\n2. Precio actual: {current_price}")
                try:
                    new_cost = float(input("Introduce el nuevo costo (o Enter para omitir): ") or current_price)
                except ValueError:
                    new_cost = current_price
                    print("Valor inválido, se mantendrá el costo actual")
                
                try:
                    margin = float(input("Introduce el margen a aplicar (por ejemplo, 2.2): ") or 2.2)
                except ValueError:
                    margin = 2.2
                    print("Valor inválido, se utilizará el margen por defecto de 2.2")
                
                if new_cost != current_price or margin != 2.2:
                    print(f"Actualizando costo a: {new_cost} y precio con margen {margin}")
                    success = shopify.update_variant_price(product_id, variant_id, new_cost, margin)
                    if success:
                        print(f"✅ Precio y costo actualizados: Precio = {round(new_cost * margin, 2)}, Costo = {new_cost}")
                        response = input("¿Deseas restaurar el costo y precio original? (s/n): ")
                        if response.lower() == 's':
                            shopify.update_variant_price(product_id, variant_id, current_price, 2.2)
                            print(f"✅ Costo restaurado a {current_price}")
                    else:
                        print("❌ Error actualizando precio y costo")
                
                # 3. Probar inventario
                inventory_item = variant.get('inventoryItem', {})
                if inventory_item and inventory_item.get('inventoryLevels', {}).get('edges'):
                    print("\nInformación detallada del inventario:")
                    print(f"InventoryItem completo: {inventory_item}")
                    
                    inventory_level = inventory_item['inventoryLevels']['edges'][0]['node']
                    print(f"Inventory Level: {inventory_level}")
                    
                    location = inventory_level['location']
                    print(f"Location: {location}")
                    
                    inventory_item_id = inventory_item['id'].split('/')[-1]
                    location_id = location['id'].split('/')[-1]
                    
                    print(f"\nIDs extraídos:")
                    print(f"inventory_item_id: {inventory_item_id}")
                    print(f"location_id: {location_id}")
                    
                    # Obtener stock actual
                    current_stock = shopify.get_inventory_level(inventory_item_id, location_id)
                    if current_stock is not None:
                        print(f"\n3. Stock actual en Shopify: {current_stock}")
                        
                        try:
                            new_stock = int(input("Introduce el nuevo stock (o Enter para omitir): ") or current_stock)
                        except ValueError:
                            new_stock = current_stock
                            print("Valor inválido, se mantendrá el stock actual")
                        
                        if new_stock != current_stock:
                            print(f"Actualizando stock a: {new_stock}")
                            success = shopify.update_inventory_quantity(
                                inventory_item_id=inventory_item_id,
                                location_id=location_id,
                                desired_quantity=new_stock
                            )
                            
                            if success:
                                print(f"✅ Stock actualizado a {new_stock}")
                                response = input("¿Deseas restaurar el stock original? (s/n): ")
                                if response.lower() == 's':
                                    success = shopify.update_inventory_quantity(
                                        inventory_item_id=inventory_item_id,
                                        location_id=location_id,
                                        desired_quantity=current_stock
                                    )
                                    if success:
                                        print(f"✅ Stock restaurado a {current_stock}")
                                    else:
                                        print("❌ Error restaurando stock original")
                            else:
                                print("❌ Error actualizando stock")
                    else:
                        print("❌ No se pudo obtener el stock actual")
            else:
                print("❌ No se encontraron variantes para este producto")
        else:
            print("❌ No se pudo obtener el producto")
            
    except Exception as e:
        print(f"\n❌ Error en las pruebas: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_shopify_api()
