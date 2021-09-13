# https://linuxize.com/post/how-to-install-python-3-7-on-debian-9/
import api_trello_class as atrc
import pandas as pd
import numpy as np
import constantesconfig as cc
import time
from datetime import datetime, timedelta
import logging
import os
from pprint import pprint
# import projeto
import cloudsqlproxy as csql
import trelloociosidade as tro
import googlechatlog as gcl
import api_firebase as afb

#quadro = 'Operação'
config = {'apikey':cc.trelloApiKey,'token':cc.trelloToken}

INTERVALO = 300

FINALIZADOS = 'Finalizados'
LISTASOPERACAO = ['E-mails','Atendimento ao Cliente','Reavaliação','Criação de Pastas','Pastas a Analisar',
                  'SICAQs a Rodar','Outros','Conferência e Envio',
                  'Geração de Formulários','Montagem de Dossiês','Crítica','Outros Repasse','Finalizados']
LISTASSUCESSO = ['E-mails Repasse','Pendência para Gerar Formulários','Formulários Enviados','Formulários Com Pendências',
                 'Reavaliação','Atendimento em Standby','Finalizados']

LISTASEXCLUSAO = ['Finalizados','E-mails','Outros Repasse','Modelos de Cartões']
# 'Reavaliação','Outros','Geração de Formulários','Crítica','Conferência e Envio','Montagem de Dossiês',

#LISTASTESTE = ['Daniel']
LISTASTESTE = ['Pastas a Analisar', 'SICAQs a Rodar',]
LISTASFILAS = ['Formulários Enviados']

LISTAS  = LISTASTESTE

CAMPOSLISTA = ['_Data','_Inicio','_Fim','_Tempo','_Lista','_Colab','_Quantidade']

LISTASDESPRIORIZAR = []

MEMBERS = ['','']

# Até 1 dia - Até 2 dias- Até 3 dias- Até 4 dias- Até 5 diasc - Acima de 5 dias
FAIXASSUCESSO = ['Até 1 dia', 'Até 2 dias', 'Até 3 dias', 'Até 4 dias', 'Até 5 dias', 'Acima de 5 dias']

r = 'teste'
def main(operacaoOuSucesso, mySQLdatabase, LISTAS):
    print('------ iniciando - versao 7/11/20-------')
    time.sleep(2)
    if operacaoOuSucesso == 'SUCESSO':
        quadro = 'Sucesso do Cliente'
        CARDSACTIONSTABLE = 'CARDS_ACTIONS_SC'
    elif operacaoOuSucesso == 'OPERACAO':
        quadro = 'Operação'
        CARDSACTIONSTABLE = 'CARDS_ACTIONS_OP'
    else:
        quadro = 'TrelloTest'
        CARDSACTIONSTABLE = 'CARDS_ACTIONS_CUM'

    qo = atrc.Trello_Board(quadro, config)

    while True:
        print('---- main loop -----')

        try:
            listid = cc.list_id_arquivar
            qo.archive_all_cards_in_list(listid)
        except Exception as e:
            erro = 'Except archive cards: ' + str(e)
            gcl.google_chat_log(erro, 'errosBots')

        data = {'atualizacao':datetime.now(),'status':'online','info':'em execucao'}
        afb.update_collection('botsStatus','trelloMonitor',data)
        dictConfig = afb.get_document_by_name_from_collection('configTrello', 'trelloMonitorConfig')
        try:
            INTERVALO = 60 * dictConfig.get('INTERVALOMINUTOS',3)
        except:
            INTERVALO = 121
        print('Intervalo: ' + str(INTERVALO))

        horaInicio = dictConfig.get('horaInicio',8)
        horaFim = dictConfig.get('horaFim',19)
        fuso = dictConfig.get('FUSO',0)
        modoTeste = dictConfig.get('TESTE',False)

        dttime = datetime.now()

        # no servidor, horario eh GMT, 3h a frente. Diminuir fuso para ter hora local.
        # Se rodar localmente, fuso deve ser 0
        horaConsiderandoFuso = dttime.hour + fuso
        datetimenow = dttime.strftime('%y%m%d%H%M')

        # executa somente se entre o horario definido
        if horaConsiderandoFuso >= horaInicio and horaConsiderandoFuso <= horaFim:
            today = datetime.now().strftime('%y%m%d')
            dayMonthYear = datetime.now().strftime('%d/%m/%Y')

            #print('--- Quantidade de cartoes por lista ---')
            data = get_quantity_listas_operacao(qo, operacaoOuSucesso)
            # print(f"quantity in lists: {data}")
            r = afb.save_info_firestore('configTrello','cartoesPorLista',data)

            datetimenow = datetime.now().strftime('%d/%m/%y %H:%M')
            data = {'horaExecucao':datetimenow}
            r = afb.save_info_firestore('configTrello', 'horaExecucao', data)
            #r = afb.save_info_subcollection_firestore('infoOperacao','resumos','cartoesPorLista'+today, datetimenow,data)
            print(r)

            print('==== lista despriorizar empreendimento ====')
            lista = afb.get_data_from_collection('despriorizarEmpreendimento','empreendimentos')
            LISTADESPRIORIZAREMPREENDIMENTO = lista.get('nomes')
            print(LISTADESPRIORIZAREMPREENDIMENTO)

            print('==== CARDS IN LIST =====')
            ###  loop pelas listas do quadro ###
            if modoTeste:
                dictLISTAS = [{'name': 'Daniel', 'id': '5f2ea637bc1e7d1f0a0836a0', 'closed': False}]
                canalChat = 'errosBots'
            else:
                dictLISTAS = qo.get_board_listsDict()
                canalChat = 'atrasos' #'AvisosBots'

            #print(dictLISTAS)
            for item in dictLISTAS:
                lista = item.get('name')
                listClosed = item.get('closed')
                if lista in LISTASEXCLUSAO or listClosed == True:
                    # LISTAEXCLUSAO no inicio do codigo
                    print('lista exclusao: '+lista)
                else:
                    print('lista analisada:'+lista)
                    listid = item.get('id') #qo.get_listid_by_name(lista)
                    print('===== GET CARDS IN LIST =====')
                    rstatus_code, rjsonCard = qo.get_cards_in_list(listid)
                    if rstatus_code == 200:
                        if len(rjsonCard) == 0:
                            print('---- lista vazia, proximo...')
                        else:
                            print('--- lista com cartoes ---')
                            #print(rjsonCard)
                    else:
                        print('----- erro status code na lista: ' + str(listid))
                        print(str(rjsonCard))
                        print('*****************************')
                        break

                    # CARDS DATA
                    for card in rjsonCard:
                        print('========= CARDS ===========')
                        columns = ['cardidIndex','name', 'cardid', 'shortUrl', 'idList']
                        data = [[card.get('id'),card.get('name'),card.get('id'),card.get('shortUrl'),card.get('idList')]]
                        dfcards = pd.DataFrame(data, columns=columns)
                        dfcards.set_index('cardidIndex', inplace=True, drop=True)

                        print('==== INSERT CARDS_BASE ====')
                        #### INCLUSOES DADOS DO CARTAO NA CARDS_BASE #############################
                        cardid, cardName, cardShortUrl = card.get('id'),card.get('name'),card.get('shortUrl')
                        #!TESTE r = csql.iwd_cardid_cards_base(cardid, cardName, today, mySQLdatabase)
                        ##########################################################################
                        if r == 'ok':
                            pass
                        else:
                            print(r)
                            print('********************** ERRO ********************')
                            time.sleep(5)

                        print('---- labels ----')
                        labels = card.get('labels')
                        dfcards, dflabels, listLabelsNamesModified, dfcardlabels, r, strLabels = insert_labels(cardid, labels, dfcards, mySQLdatabase)
                        #print(r)

                        print('---- cards actions ----')
                        r, rjson = qo.get_card_actions_moves_creation(cardid)
                        #print(r)

                        if r == 200:
                            actionsList, columnsList = qo.extract_card_listMoves_from_actions_json(rjson)
                            dfactions = create_dfActions(actionsList, columnsList)
                            #dfactions columns:
                            # ['actionid','cardid','nomeCard','idListCard','shortLink','listBefore','listAfter','dataAction','criador','idCriador',
                            #                'dia','hora','acao']
                            #nao ordenar antes do calculo de tempo da lista atual

                            print('======= TAG atraso (bizonho) ===========')
                            r = calculaTempoListaAtual(dfactions, operacaoOuSucesso, qo, cardName, cardShortUrl, LISTAS, strLabels, canalChat, fuso)
                            print(r)

                            # this func does not save data in SQL
                            print('==== MAIN FUNCTION CALCULA TEMPO EM LISTA =====')
                            #!TESTE dfcards, dfactions = geraActionsCalculaTempoEmLista(dfcards, dfactions, dfcardlabels, operacaoOuSucesso, FINALIZADOS)

                            print('---- dealing with CARDS_ACTIONS (delete by cardid) -----')
                            print(listLabelsNamesModified)
                            if len(listLabelsNamesModified) > 0:
                                #!TESTE r = csql.check_and_add_new_columns(listLabelsNamesModified, mySQLdatabase, CARDSACTIONSTABLE)
                                print('LABELS COLUMNS ADDED TO CARDS ACTIONS: ')
                                print(r)

                            #!TESTE r = csql.delete_by_id(cardid, mySQLdatabase, CARDSACTIONSTABLE)
                            #print(f'delete cards_actions: {r}')

                            print('==== INSERT CARDS ACTIONS and rename columns ====')
                            #!TESTE insert_cards_actions_SQL(dfcards, mySQLdatabase, CARDSACTIONSTABLE, qtd=0)
                            #print(f'insert cards_actions: {r}')

                            print('==== INSERT ACTIONS ====')
                            dfactions.to_excel('dfactionslast.xlsx')
                            #!TESTE r = insert_actions_in_SQL(dfactions, mySQLdatabase, dayMonthYear)
                            print(r)

                            # SAVE TO EXCEL
                            dfcards.to_excel('dfcardstemp.xlsx')
                            dfactions.to_excel('dfactionstemp.xlsx')
                        else:
                            print(rjson)
                        if lista == FINALIZADOS:
                            ### Arquiva cartoes da lista finalizados ###
                            print('---- ARQUIVAR ----')
                            #qo.archive_card(cardid)
            r = csql.get_save_tables_online()
            print(r)

            print('--- MEMBERS ACTIONS ---')
            for member in MEMBERS:
                boardmembers = qo.get_board_members()
                for item in boardmembers:
                    if member == item.get('fullName'):
                        memberid = boardmembers.get('id')
                        get_member_actions_data(memberid, qo, LISTAS)

        # loop para aguardar proxima execucao
        for i in range(1,INTERVALO):
            time.sleep(1)
            print('ainda faltam ' + str(INTERVALO - i) +' segundos         ', end='\r')

def insert_cards_actions_SQL(dfcards, mySQLdatabase, CARDSACTIONSTABLE, qtd=0):
    #print('--- cards actions SQL ---')
    dfcards.rename(columns={"cardid": "Cartao_ID", "shortUrl": "Cartao_Link",
                            'idList': 'Cartao_Lista_ID', "name": "Cartao_Nome"}, inplace=True)
    r = csql.insert_data(dfcards, mySQLdatabase, CARDSACTIONSTABLE)
    if '(1054, "Unknown column' in r:
        qtd += 1
        column = r.split("'")[1]
        print(column)
        r = csql.add_column(column,mySQLdatabase, CARDSACTIONSTABLE, 25)
        if qtd < 20:
            insert_cards_actions_SQL(dfcards, mySQLdatabase, CARDSACTIONSTABLE, qtd)
        else:
            r = 'ERRO ADD COLUMNS CARD ACTIONS'
    return r


def insert_labels(cardid,labels,dfcards, mySQLdatabase):
    print('====== save_labels ========')
    listCardidLabels = []
    listLabelsNamesModified = []
    strLabels = ''
    r = 'ok'
    for label in labels:
        labelName = label.get('name')
        print(labelName)
        #print(cardid)
        if strLabels == '':
            strLabels = labelName
        else:
            strLabels = strLabels + ',' + labelName
        listCardidLabels.append([cardid,labelName])
        #### SALVA NO MYSQL ############################
        #!TESTEr = csql.iwd_cards_labels(cardid, labelName, mySQLdatabase)
        ################################################
        if r == 'ok':
            print('insert card_labels ok')
        else:
            print('Except insert labels: ' + str(r))
            erro = 'Except insert labels: ' + str(r)
            gcl.google_chat_log(erro, 'errosBots')
        ### insere labels na df cards
        try:
            labelName = removecaractereslatinos(labelName)
            labelNameModified = labelName.replace(" ", "")
            dfcards[labelNameModified] = '1'
            listLabelsNamesModified.append(labelNameModified)
        except Exception as e:
            print('EXCEPT (save_labels): ' + str(e))
            gcl.google_chat_log('EXCEPT (save_labels): ' + str(e), 'errosBots')
            r = 'erro'
    dflabels = pd.DataFrame(listCardidLabels, columns=['cardid','label'])
    data=[[cardid, cardid, strLabels]]
    dfcardlabels = pd.DataFrame(data, columns=['cardidIndex','cardid','labels'])
    dfcardlabels.set_index('cardidIndex', inplace=True, drop=True)
    return dfcards, dflabels, listLabelsNamesModified, dfcardlabels, r, strLabels


def geraActionsCalculaTempoEmLista(dfcards, dfactions, dfcardlabels, operacaoOuSucesso, lastList=FINALIZADOS):
    # sort
    dfactions.sort_values(by=['dataAction'], ascending=[True], inplace=True)
    dfactions.update(dfcardlabels)
    dfactions.to_excel('ddfactionscardslabels.xlsx')
    dfcardlabels.to_excel('ddfcardlabels.xlsx')

    #print(df)
    cardid = ''
    listBerforeLastRow = ''
    listAfterLastRow = ''
    dataActionLastRow = '' #cardCreationDate
    horaLastRow = ''
    qtdMoves = len(dfactions.index)
    tempoTotal = 0
    print('--- itertuples ---')
    for r in dfactions.itertuples(index=False):
        #print(r)
        listName = r.listBefore
        print('r.acao : ' + str(r.acao) )
        if True: #try:
            if r.acao == 'createCard':
                # card basic data
                dataCard={'cardid':[r.cardid],'Cartao_Data':[r.dia],'Cartao_Hora':[r.hora],'Operacao':operacaoOuSucesso}
                dfcreateCardtemp = pd.DataFrame(dataCard)  # ,columns=['actionid','ociosidade'])

                # merge does not need index
                dfcards = dfcards.merge(dfcreateCardtemp, how='left', on='cardid')
                #dfcards.to_excel('ddfcardsupdateCartaoData.xlsx')
                #dfcreateCardtemp.to_excel('dfcreatecardTemp.xlsx')
            else:
                try:
                    rowDataAction = datetime.strptime(r.dataAction, "%Y-%m-%dT%H:%M:%S.%fZ")
                    lastRowDataAction = datetime.strptime(dataActionLastRow, "%Y-%m-%dT%H:%M:%S.%fZ")

                    # tempo desta acao
                    timeInList = rowDataAction - lastRowDataAction

                    # tempo total acumulado de cada iteracao
                    tempoTotal = tempoTotal + timeInList.total_seconds()

                    # time in list in format HH:MM:SS
                    hora, restosegundos = divmod(timeInList.total_seconds(), 3600)
                    minutos, segundos = divmod(restosegundos, 60)
                    timeListHMS = str(int(hora)).zfill(2) + ":" + str(int(minutos)).zfill(2) + ":" + str(
                        int(segundos)).zfill(2)
                except Exception as e:
                    print(str(e))
                    print('EXCEPT '+ str(rowDataAction))
                    timeInList = 0
                    timeListHMS = '00:00:00'

                ### conta quantas vezes tem a acao na listBefore para calcular a qtd ###
                dfCol = dfactions.loc[dfactions['listBefore']==r.listBefore]
                lenCol = len(dfCol.index)
                col = removecaractereslatinos(r.listBefore)

                # change day format from YYYY-MM-DD to DD/MM/YYYY
                dayDMY = r.dia[-2:] + "/" + r.dia[5:7] + "/" + r.dia[:4]

                ##### ATUALIZA dfcards ######  # ['_Data','_Inicio','_Fim','_Tempo','_Lista','_Colab','_Quantidade']
                if col in LISTASSUCESSO: #(include only columns of actions)
                    data = {'cardidIndex': [r.cardid],'cardid': [r.cardid], col+'_Data':[dayDMY],
                        col+"_Inicio":[horaLastRow], col+"_Fim":[r.hora], col+'_Tempo':[timeListHMS],
                        col+'_Lista':[r.listAfter], col+'_Colab':[r.criador], col+'_Quantidade':[lenCol],
                        col+'_ActionId':[r.actionid]}

                    #print('DATA dfactions:')
                    #print(data)
                    dfdata = pd.DataFrame(data)  # ,columns=['actionid','ociosidade'])
                    dfdata.set_index('cardidIndex', inplace=True,drop=True)

                    dfcardscolumns = dfcards.columns
                    # update by update or merge
                    if col+'_Data' in dfcardscolumns:
                        # if column already exists
                        dfcards.update(dfdata)
                    else:
                        dfcards = dfcards.merge(dfdata,how="left",on="cardid")

                #print('===== UPDATE for ACTION ID =========')
                #print('*** CRIAR COLUNA DIA FIM ***')
                try:
                    tempoAcao = timeInList.total_seconds()
                except:
                    tempoAcao = 0
                dataAction={'actionid':[r.actionid],'Acao_Inicio':[horaLastRow],'Acao_Tempo':[tempoAcao],'dia':[dayDMY]}
                #'Acao':[r.listBefore],

                dfdataAction = pd.DataFrame(dataAction)
                dfdataAction.set_index('actionid', inplace=True, drop=True)

                dfactions.set_index('actionid', inplace=True, drop=True)
                #print(dfdataAction)
                #print(dfactions[['Acao','Acao_Tempo','Acao']])
                dfactions.update(dfdataAction)
                #print(dfactions[['Acao', 'Acao_Tempo', 'Acao']])
                dfactions.reset_index('actionid', inplace=True, drop=False)
        else: #except Exception as e:
            print(str(e))
            time.sleep(5)
        dataActionLastRow = r.dataAction
        #csql.insert_action(row.actionid, row.cardid, listName, timeInList)
        # atualizar dados da ultima linha
        listBerforeLastRow = r.listBefore
        listAfterLastRow = r.listAfter
        dataActionLastRow = r.dataAction
        horaLastRow = r.hora
        cardid = r.cardid
    if operacaoOuSucesso == 'SUCESSO':
        faixa_tempo = calcula_faixa_tempo_dias(tempoTotal)
        dataTempo = {'cardid': cardid, 'Tempo_Total': [tempoTotal], 'Faixa_Dias': [faixa_tempo]}
    else:
        faixa_tempo = 'criar fc para operacao'
        dataTempo = {'cardid':cardid,'Tempo_Total': [tempoTotal],'Faixa_Tempo':[faixa_tempo]}
    dfdataTempo = pd.DataFrame(dataTempo)  # ,columns=['actionid','ociosidade'])
    #dfdataTempo.set_index('cardid',inplace=True)
    #dfdataTempo.to_excel('ddfdataTempo.xlsx')
    #dfcards.to_excel('ddfcards.xlsx')
    print('==== MERGE dfdataTempo ====')
    #print(dfcards)
    #print(dfdataTempo)
    #dfcards.reset_index(inplace=True,drop=True)
    dfcards = dfcards.merge(dfdataTempo,how='inner', on='cardid')
    #dfcards.set_index('cardid',inplace=True, drop=False)
    print('===== merge dfdatatempo')

    print(qtdMoves)
    print(str(tempoTotal))
    #
    dfcards.to_excel('dfcardsIII.xlsx')
    # labels_list, ociosidade , Data_Fim
    return dfcards, dfactions


def insert_actions_in_SQL(dfactions, mySQLdatabase, dayMonthYear):
    print('====================== insert ACTIONS =====================')
    dfactions.to_excel('dfactionslast.xlsx')
    for row in dfactions.itertuples(index=False):
        r = csql.delete_by_id(row.actionid, mySQLdatabase, 'ACTIONS')
        #print(f'delete actions: {r}')
    dfactions.rename(columns={"cardid":"Cartao_ID","shortLink":"Cartao_Link","criador":"Colaborador"},inplace=True)
    dfactions.rename(columns={"dia": "Acao_Data", "hora": "Acao_Fim", "listBefore": "Acao",'listAfter':'lista_Colab'},inplace=True)
    dfactions.rename(columns={"labels": "labels_list"},inplace=True)
    #print(dfactions.columns)
    # "actionid":"Acao_ID",
    #dfactions.to_excel("dfactions11.xlsx")
    try:
        dfactions.drop(['idListCard','nomeCard'],inplace=True, axis=1)
    except Exception as e:
        print('Exception : ' + str(e))
    try:
        dfactions.drop(['acao'], inplace=True, axis=1)
        dfactions.drop(['idCriador', 'dataAction'], inplace=True, axis=1)
    except Exception as e:
        print('Exception : ' + str(e))
    # "nomeCard":"NomeCartao","idListCard":

    dfactionsFinal = dfactions[['actionid','Cartao_ID','Cartao_Link','Colaborador','lista_Colab','Acao','Acao_Data',
                                'Acao_Inicio','Acao_Fim','Acao_Tempo','Ociosidade','labels_list']]
    dfactionsFinal.to_excel('dfact.xlsx')
    dfactionsFinal = tro.mainociosidade(dfactionsFinal,'nomeArquivoActions','',True, dayMonthYear)
    # inclui na CARDS_ACTIONS info do CARDID
    print('---- insert_data ----')
    print(operacaoOuSucesso)
    r = csql.insert_data(dfactionsFinal, mySQLdatabase, 'ACTIONS')
    print(r)
    print('--- get SQL ONLINE database data ---')
    r = csql.get_save_tables_online_actions()
    #print(f'insert actions: {r}')
    ########################################################################
    return 'ok'


def create_dfActions(actionsList, columnsList):
    print('===== createActions ====')
    #print(actionsList)
    #print(columnsList)
    df = pd.DataFrame(actionsList, columns=columnsList)
    df['cardidIndex'] = df['cardid']
    #df['Acao'] = np.nan
    df['Acao_Inicio'] = np.nan
    df['Acao_Tempo'] = np.nan
    df['Ociosidade'] = np.nan
    df['labels'] = np.nan

    df.set_index('cardidIndex', inplace=True, drop=True)
    df.to_excel('dfactionsOriginal.xlsx')
    return df

def calculaOciosidade(df):
    print('implementar calcula ociosidade')
    # data = {'actionid': [row.actionid], 'ociosidade': [ociosidadestr]}

def calculaTempoListaAtual(dfactions, OPouSC, qo,cardName, cardShortUrl, actionLists, labelsExistentes, canalChat, FUSO):
    #TODO print('--- TAG delay (Bizonho) ---')

    # tem que analisar a linha da ultima acao do cartao
    # essa linha tem a lista atual
    if dfactions.empty:
        print('EMPTY DATA FRAME')
        return 'ok'

    # verificar se esse cartao tem informacao sobre ultima tag de atraso no banco
    # retirar tag, se houver tag e lista
    print(dfactions.head())
    lastMove = dfactions.iloc[0]['dataAction']
    lista = dfactions.iloc[0]['listAfter']
    listBefore = dfactions.iloc[0]['listBefore']
    cardid = dfactions.iloc[0]['cardid']
    print('TAG TEMPO: listBefore,lista e lastMove: '+str(listBefore)+";"+str(lista)+";"+str(lastMove))
    status = 'ok'

    # tempos limites das filas e valor do fUSO horario (uso local ou no servidor)
    dictTempoFilas = afb.get_document_by_name_from_collection('configTrello', 'prazoFilas')
    print(dictTempoFilas)

    # verificar se existe label de atraso, se listBefore do controle eh diferente da ultima acao
    # se for diferente, retirar a label de atraso e deletar do controle
    dataControle = afb.get_document_by_name_from_collection('cartoesComTagAtraso',cardid)

    if dataControle is not None:
        print(dataControle)
        if dataControle.get('listBefore') == listBefore:
            print('mesma lista do controle, nao apagar, cartao continua na mesma lista em que foi colocada a label')
        else:
            # lista mudou entao nao preciso mais marcar o atraso
            print('lista mudou, retirar a label de atraso')
            afb.delete_document_from_collection('cartoesComTagAtraso',cardid)
            labelAtraso = dictTempoFilas.get('labelAtraso','')
            r = qo.remove_label(cardid, labelAtraso)
            print("resposta remove label: " + str(r))
            print(r)

    # lista eh listAfter
    if lista not in actionLists and lista != FINALIZADOS:
        status = 'ok'

        # ignora cartoes vindos de Finalizados e E-mails
        if listBefore in actionLists and listBefore not in ['Finalizados','E-mails','E-mails Repasse']:
            now = datetime.now()
            # adicionando Fuso horario
            # FUSO DEVE SER 0 SE RODAR NA NUVEM E DIFERENTE DE ZERO SE RODAR LOCAL
            #FUSO = dictTempoFilas.get('FUSO',0)
            now = now + timedelta(hours=FUSO)

            # diferenca entre agora e data do ultimo move do cartao
            diff = now - datetime.strptime(lastMove, "%Y-%m-%dT%H:%M:%S.%fZ")
            print('now, lastMove, diff:'+str(now)+";"+lastMove+";"+str(diff))

            # limite de tempo grande caso nao encontre a lista no Firebase
            prazoListaMinutos = dictTempoFilas.get(listBefore,50000)
            print('prazoListaMinutos:'+str(prazoListaMinutos))

            if diff.total_seconds() < 0:
                print('ERRO no calculo diff')
                msg = 'TAG BIZONHO diff menor que zero ERRO NO FUSO' + str(diff) + ";" + cardName
                gcl.google_chat_log(msg, 'errosBots')
            elif diff > timedelta(minutes=prazoListaMinutos):
                if cardName.startswith('Escala:'):
                    print('cartao de escala')
                else:
                    # add_label(Atraso)
                    labelAtraso = dictTempoFilas.get('labelAtraso','')
                    # verifica se o label de atraso ja existe, para nao adicionar 2x no chat
                    if labelAtraso in labelsExistentes:
                        print('label ja adicionado, card:'+cardName)
                    else:
                        # adicioar label de atraso:
                        qo.add_label(cardid ,labelAtraso)
                        # adicionar comentario
                        comentario = 'Tempo excedido em ' + str(listBefore)
                        qo.add_comment(cardid, comentario)
                        # adicionar cardid, lista e label no controle do firebase de cartoes com tag:
                        dataCartao = {'listBefore':listBefore,'label':labelAtraso,'dia':datetime.now().strftime("%Y-%m-%d")}
                        afb.save_info_firestore('cartoesComTagAtraso',cardid,dataCartao)

                        diff = str(diff).split(".")[0]
                        #CARD ESTOURADO: Nome do Colaborador(Lista) | Etapa da Esteira | Tempo do cartão(HH: MM:SS) | Nome do Cartão | Link Cartão
                        #msg = 'CARD ESTOURADO: ' + cardName + '; em: ' + lista + '; operacao: ' + listBefore + '; tempo: ' + str(
                        #    diff) + '; ' + cardShortUrl + "; now: " + now.strftime("%Y-%m-%d %H:%M:%S") + "; hr lastMove: " + lastMove
                        msg='CARD ESTOURADO: '+lista+' | '+listBefore+' | '+str(diff)+' | '+cardName+' | '+cardShortUrl
                        gcl.google_chat_log(msg, canalChat)
                        print(msg)
                        status = 'late'
                        time.sleep(10)
            else:
                print('cartao no prazo na lista:'+str(cardName))
        else:
            print('listBefore not in actionlists:'+str(listBefore))
    return status

def get_quantity_listas_operacao(qo, operacaoOuSucesso):
    #### GET QTTYS ####
    #dataqtdList
    x = []
    if operacaoOuSucesso == 'OPERACAO':
        listas = LISTASOPERACAO
    elif operacaoOuSucesso == 'SUCESSO':
        listas = LISTASSUCESSO
    else:
        listas = []
    for item in listas:
        listid = qo.get_listid_by_name(item)
        qtd = qo.get_qty_cards_in_list(listid)
        x.append(item)
        x.append(qtd)
    data = dict(x[i:i + 2] for i in range(0, len(x), 2))
    return data

def despriorizarEmpreendimentos(qo):
    listadict = afb.get_document_by_name_from_collection('configSistemaConstrutoras', 'despriorizarEmpreendimento')
    listaEmpreendimentos = listadict.get('empreendimentos')
    listaListas = listadict.get('listas')
    print(listaListas)
    print(listaEmpreendimentos)
    for listName in listaListas:
        listid = qo.get_listid_by_name(listName)
        #print(f'listid: {listid}')
        rstatus, cardsJson = qo.get_cards_in_list(listid)
        print(cardsJson)
        if len(cardsJson) > 0 and rstatus == 200:
            for card in cardsJson:
                name = card.get('name')
                empreendimentoName = name.split("-")[0].strip()
                print(empreendimentoName)
                if empreendimentoName in listaEmpreendimentos:
                    print('---- DESPRIORIZAR: ' + str(empreendimentoName) +' ----')
                    cardid = card.get('id')
                    rstatus, rtext = qo.move_card_bottom(cardid)
                    if rstatus == 200:
                        print('despriorizacao ok: {name}')
                    else:
                        print(rtext)
        else:
            print('empty list: {listName}, {rstatus}')
        time.sleep(0.2)

'''
def save_actions(df,quadro):
    print('save info')
    csql.insert_cards_actions(df,quadro)
'''

def get_member_actions_data(memberid, qo, actionLists):
    print('--- get_member_actions_data ---')
    print(memberid)
    rstatuscode, rjson = qo.get_member_actions(memberid)
    print(rstatuscode)
    now = datetime.now()
    dateMoveBefore = ''
    for item in rjson:
        data = item.get('data', 'na')
        if data != 'na':
            if data.get('listBefore', 'na') != 'na':
                print(item)
                nameCard = data.get('card').get('name')
                listBefore = data.get('listBefore').get('name')
                listAfter = data.get('listAfter').get('name')
                listidAfter = data.get('listAfter').get('id')
                dateMove = item.get('date')
                diff = now - datetime.strptime(dateMove, "%Y-%m-%dT%H:%M:%S.%fZ")
                print(diff)
                print(listBefore)
                print(listAfter)
                print(nameCard)
                if diff > timedelta(minutes=10):
                    print('prazo <')
                else:
                    print('prazo >')
                try:
                    if dateMoveBefore == '':
                        pass
                    else:
                        diffBetweenMoves = datetime.strptime(dateMove, "%Y-%m-%dT%H:%M:%S.%fZ") \
                                           - datetime.strptime(dateMoveBefore, "%Y-%m-%dT%H:%M:%S.%fZ")
                except Exception as e:
                    print(str(e))
                dateMoveBefore = dateMove
                if listBefore in actionLists:
                    print('ação: ' + str(listBefore))
                    rstatus, cards = qo.get_cards_in_list(listidAfter)
                    for card in cards:
                        cardName = card.get('name', '')
                        if cardName.startswith('Escala:'):
                            print('lista de usuario: ' + str(listAfter))
                            print('cartao: ' + str(cardName) + ";" + str(listBefore))
                            time.sleep(5)
    return 'ok'

#### #######################################
#### fcs de apoio
def calcula_faixa_tempo_dias(tempoTotal):
    umdiaemsegundos = 24*3600
    dias = tempoTotal/umdiaemsegundos
    # Até 1 dia - Até 2 dias- Até 3 dias- Até 4 dias- Até 5 diasc - Acima de 5 dias
    if dias < 1:
        faixa = 'Até 1 dia'
    elif dias < 2:
        faixa = 'Até 2 dias'
    elif dias < 3:
        faixa = 'Até 3 dias'
    elif dias < 4:
        faixa = 'Até 4 dias'
    elif dias < 5:
        faixa = 'Até 5 dias'
    else:
        faixa = 'Acima de 5 dias'
    return faixa

def removecaractereslatinos(item):
    newitem = item.replace("ç", "c")
    newitem = newitem.replace("ã", "a")
    newitem = newitem.replace("á", "a")
    newitem = newitem.replace("à", "a")
    newitem = newitem.replace("é", "e")
    newitem = newitem.replace("ó", "o")
    newitem = newitem.replace("ê", "e")
    newitem = newitem.replace("ú", "u")
    newitem = newitem.replace("û", "u")
    newitem = newitem.replace("í", "i")
    newitem = newitem.replace("|", "")
    newitem = newitem.replace("-", "")
    newitem = newitem.replace("(", "")
    newitem = newitem.replace(")", "")
    newitem = newitem.replace(" ", "")
    return newitem




if __name__=='__main__':
    #operacaoOuSucesso = 'Sucesso do Cliente'
    operacaoOuSucesso = 'OPERACAO' #'SUCESSO'
    mySQLdatabase = 'ONLINE'
    if operacaoOuSucesso == 'OPERACAO':
        listas = LISTASOPERACAO
    elif operacaoOuSucesso == 'SUCESSO':
        listas = LISTASSUCESSO
    else:
        listas = []
    while True:
        try:
            main(operacaoOuSucesso, mySQLdatabase, listas)
        except Exception as e:
            print('***** EXCEPT MAIN:' + str(e))
            erro = '***** EXCEPT MAIN:' + str(e)
            data = {'atualizacao': datetime.now(),'status':'offline','info': 'EXCEPTION ' + str(e)}
            afb.update_collection('botsStatus', 'trelloMonitor', data)
            try:
                gcl.google_chat_log(erro, 'errosBots')
            except:
                pass
            print('AGUARDANDO 60s para reiniciar')
            time.sleep(60)


