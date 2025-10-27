class UpsertCollection:
    def __init__(self, api):
        self.api = api
        self.key_name = ""

        self.items_to_insert = []
        self.items_to_update = []


    def upsert(self, collection):
        self.api.create(collection, self.items_to_insert)
        self.api.update(collection, self.items_to_update, self.key_name)


    def merge_items_by_key(self, new_items, duplicate_items, key_name):
        """
        Mescla new_items com duplicate_items (old_items) baseado em uma chave.
        
        Args:
            new_items: Lista de dicionários com dados novos/atualizados
            duplicate_items: Lista de dicionários com dados existentes (contém 'id')
            key_name: Nome da chave para identificar duplicatas (ex: 'nfe_key')
            
        Returns:
            tuple: (items_to_insert, items_to_update)
                - items_to_insert: Novos itens que não existem em duplicate_items
                - items_to_update: Itens existentes atualizados com dados de new_items (mantém o 'id')
                                   APENAS se houver mudança em algum campo (com conversão)
        """
        self.key_name = key_name

        # Criar um dicionário de lookup para acesso rápido aos itens antigos pela chave
        old_items_map = {item[key_name]: item for item in duplicate_items}
        
        for new_item in new_items:
            key_value = new_item.get(key_name)
            
            if key_value in old_items_map:
                # Item já existe - verificar se houve mudança
                old_item = old_items_map[key_value]
                
                # Verificar se algum campo de new_item mudou em relação ao old_item
                has_changes = False
                for field, new_value in new_item.items():
                    old_value = old_item.get(field)
                    
                    # Comparar valores com conversão
                    if self._values_are_different(old_value, new_value):
                        has_changes = True
                        break
                
                # Só adiciona para atualização se realmente houver mudança
                if has_changes:
                    # Criar cópia do item antigo e atualizar com novos dados
                    updated_item = old_item.copy()
                    updated_item.update(new_item)
                    
                    # Garantir que o 'id' original seja mantido
                    updated_item['id'] = old_item['id']
                    
                    self.items_to_update.append(updated_item)
            else:
                # Item novo - inserir
                self.items_to_insert.append(new_item)


    def _values_are_different(self, old_value, new_value):
        """
        Compara dois valores considerando conversões de tipo (string vs número, etc).
        
        Returns:
            bool: True se os valores são diferentes, False se são iguais
        """
        # Se ambos são None, considerar iguais
        if old_value is None and new_value is None:
            return False
        
        # Se apenas um é None, são diferentes
        if old_value is None or new_value is None:
            return True
        
        # Tentar converter ambos para comparação
        try:
            # Tentar converter para float (para comparar números)
            try:
                old_float = float(old_value)
                new_float = float(new_value)
                # Comparar com tolerância para float
                return abs(old_float - new_float) > 1e-9
            except (ValueError, TypeError):
                # Se não for número, comparar como string
                old_str = str(old_value).strip()
                new_str = str(new_value).strip()
                
                # Normalizar timestamps (remover 'Z' no final)
                if 'T' in old_str and 'T' in new_str:
                    old_str = old_str.rstrip('Z')
                    new_str = new_str.rstrip('Z')
                    
                return old_str != new_str
        except Exception:
            # Em caso de erro, considerar diferentes
            return True