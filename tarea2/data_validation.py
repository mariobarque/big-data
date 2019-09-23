
def get_sales_data_validations():
    validations = [
        'numero_caja is not null',
        'numero_caja > 0',
        'producto is not null',
        'length(producto) > 0',
        'cantidad is not null',
        'cantidad > 0',
        'precio_unitario is not null',
        'precio_unitario > 0',
        'venta is not null',
        'venta > 0'
    ]
    return ' and '.join(validations)