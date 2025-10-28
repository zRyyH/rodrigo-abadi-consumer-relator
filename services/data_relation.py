class DataRelation:
    def __init__(self):
        pass

    def relate_sales(self, nfes, products, sales):
        """
        Relaciona sales com products e nfes.
        Retorna apenas sales que precisam de atualização (campos faltando ou relacionamento incorreto).
        """

        # Criar dicionários de mapeamento para busca rápida
        # Mapear SKU -> product_id
        sku_to_product_id = {product["sku"]: product["id"] for product in products}

        # Mapear sale_or_dispatch -> nfe_id
        sale_dispatch_to_nfe_id = {nfe["sale_or_dispatch"]: nfe["id"] for nfe in nfes}

        # Lista para armazenar apenas sales que precisam de atualização
        sales_para_atualizar = []

        for sale in sales:
            precisa_atualizar = False
            sale_atualizada = sale.copy()

            # Verificar relacionamento com product pelo SKU
            sku = sale.get("sku")
            product_id_atual = sale.get("product_id")

            if sku and sku in sku_to_product_id:
                product_id_correto = sku_to_product_id[sku]

                # Verificar se o product_id está faltando ou está incorreto
                if product_id_atual is None:
                    sale_atualizada["product_id"] = product_id_correto
                    precisa_atualizar = True
                elif product_id_atual != product_id_correto:
                    # Relacionamento incorreto, precisa corrigir
                    sale_atualizada["product_id"] = product_id_correto
                    precisa_atualizar = True

            # Verificar relacionamento com NFe pelo sale_id
            sale_id = sale.get("sale_id")
            nfe_id_atual = sale.get("nfe_id")

            if sale_id and sale_id in sale_dispatch_to_nfe_id:
                nfe_id_correto = sale_dispatch_to_nfe_id[sale_id]

                # Verificar se o nfe_id está faltando ou está incorreto
                if nfe_id_atual is None:
                    sale_atualizada["nfe_id"] = nfe_id_correto
                    precisa_atualizar = True
                elif nfe_id_atual != nfe_id_correto:
                    # Relacionamento incorreto, precisa corrigir
                    sale_atualizada["nfe_id"] = nfe_id_correto
                    precisa_atualizar = True

            # Adicionar à lista apenas se precisa de atualização
            if precisa_atualizar:
                sales_para_atualizar.append(sale_atualizada)

        return sales_para_atualizar

    def relate_nfes(self, files, nfes):
        """
        Relaciona NFes com arquivos XML e PDF baseando-se na nfe_key.
        Retorna apenas as NFes que foram relacionadas (que não estavam previamente relacionadas).

        Regras:
        - NFe só é considerada relacionada se AMBOS xml_id E pdf_id estiverem preenchidos
        - Verifica se os relacionamentos existentes estão corretos
        - Se relacionamento estiver errado, corrige e retorna a NFe
        """
        # Cria um dicionário para lookup rápido dos arquivos por filename_download
        files_dict = {file["filename_download"]: file for file in files}

        # Cria um dicionário reverso para lookup por ID
        files_by_id = {file["id"]: file for file in files}

        # Lista para armazenar apenas as NFes que foram relacionadas
        nfes_relacionadas = []

        # Atualiza cada NFe com os IDs dos arquivos correspondentes
        for nfe in nfes:
            nfe_key = nfe.get("nfe_key")

            if not nfe_key:
                continue

            # Verifica se a NFe estava COMPLETAMENTE relacionada antes (ambos xml_id E pdf_id)
            estava_relacionada = (
                nfe.get("xml_id") is not None and nfe.get("pdf_id") is not None
            )

            # Verifica se os relacionamentos existentes estão corretos
            relacionamento_correto = True

            if nfe.get("xml_id") is not None:
                xml_file = files_by_id.get(nfe["xml_id"])
                expected_xml_filename = f"{nfe_key}.xml"
                if (
                    not xml_file
                    or xml_file["filename_download"] != expected_xml_filename
                ):
                    relacionamento_correto = False
                    nfe["xml_id"] = None  # Remove relacionamento incorreto

            if nfe.get("pdf_id") is not None:
                pdf_file = files_by_id.get(nfe["pdf_id"])
                expected_pdf_filename = f"{nfe_key}.pdf"
                if (
                    not pdf_file
                    or pdf_file["filename_download"] != expected_pdf_filename
                ):
                    relacionamento_correto = False
                    nfe["pdf_id"] = None  # Remove relacionamento incorreto

            # Se o relacionamento estava incorreto, agora a NFe não está mais relacionada
            if estava_relacionada and not relacionamento_correto:
                estava_relacionada = False

            foi_relacionada = False

            # Verifica se existe arquivo XML com a nfe_key
            xml_filename = f"{nfe_key}.xml"
            if xml_filename in files_dict and nfe.get("xml_id") is None:
                nfe["xml_id"] = files_dict[xml_filename]["id"]
                foi_relacionada = True

            # Verifica se existe arquivo PDF com a nfe_key
            pdf_filename = f"{nfe_key}.pdf"
            if pdf_filename in files_dict and nfe.get("pdf_id") is None:
                nfe["pdf_id"] = files_dict[pdf_filename]["id"]
                foi_relacionada = True

            # Verifica se agora está completamente relacionada (ambos xml_id E pdf_id preenchidos)
            agora_relacionada = (
                nfe.get("xml_id") is not None and nfe.get("pdf_id") is not None
            )

            # Adiciona à lista se:
            # - Não estava relacionada antes E agora está completamente relacionada
            # - OU se o relacionamento estava incorreto e foi corrigido
            if (not estava_relacionada and agora_relacionada) or (
                not relacionamento_correto and foi_relacionada
            ):
                nfes_relacionadas.append(nfe)

        return nfes_relacionadas
