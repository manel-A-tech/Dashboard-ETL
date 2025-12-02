import streamlit as st
import plotly.graph_objects as go
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data_simple

# config de la page
st.set_page_config(
    page_title="Dashboard Northwind ETL",
    layout="wide"
)


# titre principal
st.title("Dashboard ETL - Commandes")
st.markdown("---")

# bouton de rafraîchissement en haut
if st.button(" Rafraîchir les données", type="primary"):
    with st.spinner("Extraction en cours..."):
        df_raw = extract_data()

    if not df_raw.empty:
        with st.spinner("Transformation en cours..."):
            df_transformed = transform_data(df_raw)

        with st.spinner("Chargement en cours..."):
            success = load_data_simple(df_transformed)

        if success:
            st.session_state['data'] = df_transformed
            st.success("Données mises à jour!")
        else:
            st.error("Erreur lors du chargement des données")
    else:
        st.error("Aucune donnée extraite")

# chargement initial des données
if 'data' not in st.session_state:
    with st.spinner("Chargement initial des données..."):
        df_raw = extract_data()
        if not df_raw.empty:
            df_transformed = transform_data(df_raw)
            st.session_state['data'] = df_transformed

# vérification des données
if 'data' not in st.session_state or st.session_state['data'].empty:
    st.warning("Aucune donnée disponible. Cliquez sur “Rafraîchir les données”.")
    st.stop()

df = st.session_state['data']

df_filtered = df

# KPIs principaux
st.header("Analyse des commandes livrées et non livrées")
col1, col2, col3, col4 = st.columns(4)

total_commandes = len(df_filtered)
commandes_livrees = len(df_filtered[df_filtered['Status_Livraison'] == 'Livrée'])
commandes_non_livrees = len(df_filtered[df_filtered['Status_Livraison'] == 'Non Livrée'])
taux_livraison = (commandes_livrees / total_commandes * 100) if total_commandes > 0 else 0

with col1:
    st.metric("Total Commandes", f"{total_commandes:,}")

with col2:
    st.metric("Commandes Livrées", f"{commandes_livrees:,}")

with col3:
    st.metric("Commandes Non Livrées", f"{commandes_non_livrees:,}")

with col4:
    st.metric("Taux de Livraison", f"{taux_livraison:.1f}%")

st.markdown("---")

# graphiques par dimension
tab1, tab2, tab3, tab4 = st.tabs(["Par Client", "Par Employé", "Par Mois", "Par Année"])

# ------------------ CLIENTS ------------------
with tab1:
    st.subheader("Analyse par Client")

    df_client = df_filtered.groupby(['CompanyName', 'Status_Livraison']).size().reset_index(name='Nombre')
    df_client_pivot = df_client.pivot(index='CompanyName', columns='Status_Livraison', values='Nombre').fillna(0)
    df_client_pivot['Total'] = df_client_pivot.sum(axis=1)
    df_client_pivot = df_client_pivot.sort_values('Total', ascending=False).head(15)

    fig1 = go.Figure()
    if 'Livrée' in df_client_pivot.columns:
        fig1.add_trace(go.Bar(name='Livrée', x=df_client_pivot.index, y=df_client_pivot['Livrée'], marker_color='#2dc653'))
    if 'Non Livrée' in df_client_pivot.columns:
        fig1.add_trace(go.Bar(name='Non Livrée', x=df_client_pivot.index, y=df_client_pivot['Non Livrée'], marker_color='#e74c3c'))

    fig1.update_layout(
        barmode='stack',
        title='Top 15 Clients - Commandes par Statut',
        xaxis_title='Client',
        yaxis_title='Nombre de Commandes',
        height=500
    )
    st.plotly_chart(fig1, use_container_width=True)

    with st.expander("Voir le tableau détaillé"):
        st.dataframe(df_client_pivot.reset_index(), use_container_width=True)

# ------------------ EMPLOYÉS ------------------
with tab2:
    st.subheader("Analyse par Employé")

    df_employe = df_filtered.groupby(['EmployeeName', 'Status_Livraison']).size().reset_index(name='Nombre')
    df_employe_pivot = df_employe.pivot(index='EmployeeName', columns='Status_Livraison', values='Nombre').fillna(0)
    df_employe_pivot['Total'] = df_employe_pivot.sum(axis=1)
    df_employe_pivot = df_employe_pivot.sort_values('Total', ascending=False)

    fig2 = go.Figure()
    if 'Livrée' in df_employe_pivot.columns:
        fig2.add_trace(go.Bar(name='Livrée', x=df_employe_pivot.index, y=df_employe_pivot['Livrée'], marker_color='#2dc653'))
    if 'Non Livrée' in df_employe_pivot.columns:
        fig2.add_trace(go.Bar(name='Non Livrée', x=df_employe_pivot.index, y=df_employe_pivot['Non Livrée'], marker_color='#e74c3c'))

    fig2.update_layout(
        barmode='stack',
        title='Commandes par Employé et Statut',
        xaxis_title='Employé',
        yaxis_title='Nombre de Commandes',
        height=500
    )
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Voir le tableau détaillé"):
        st.dataframe(df_employe_pivot.reset_index(), use_container_width=True)

# ------------------ MOIS ------------------
with tab3:
    st.subheader("Analyse par Mois")

    df_mois = df_filtered.groupby(['Mois_Annee', 'Status_Livraison']).size().reset_index(name='Nombre')
    df_mois_pivot = df_mois.pivot(index='Mois_Annee', columns='Status_Livraison', values='Nombre').fillna(0)
    df_mois_pivot = df_mois_pivot.sort_index()

    fig3 = go.Figure()
    if 'Livrée' in df_mois_pivot.columns:
        fig3.add_trace(go.Scatter(name='Livrée', x=df_mois_pivot.index, y=df_mois_pivot['Livrée'], mode='lines+markers'))
    if 'Non Livrée' in df_mois_pivot.columns:
        fig3.add_trace(go.Scatter(name='Non Livrée', x=df_mois_pivot.index, y=df_mois_pivot['Non Livrée'], mode='lines+markers'))

    fig3.update_layout(
        title='Évolution Mensuelle des Commandes',
        xaxis_title='Mois',
        yaxis_title='Nombre de Commandes',
        height=500
    )
    st.plotly_chart(fig3, use_container_width=True)

    with st.expander("Voir le tableau détaillé"):
        st.dataframe(df_mois_pivot.reset_index(), use_container_width=True)

# ------------------ ANNÉES ------------------
with tab4:
    st.subheader("Analyse par Année")

    df_annee = df_filtered.groupby(['Annee', 'Status_Livraison']).size().reset_index(name='Nombre')
    df_annee_pivot = df_annee.pivot(index='Annee', columns='Status_Livraison', values='Nombre').fillna(0)

    fig4 = go.Figure()
    if 'Livrée' in df_annee_pivot.columns:
        fig4.add_trace(go.Bar(name='Livrée', x=df_annee_pivot.index, y=df_annee_pivot['Livrée'], marker_color='#2dc653'))
    if 'Non Livrée' in df_annee_pivot.columns:
        fig4.add_trace(go.Bar(name='Non Livrée', x=df_annee_pivot.index, y=df_annee_pivot['Non Livrée'], marker_color='#e74c3c'))

    fig4.update_layout(
        barmode='group',
        title='Commandes par Année et Statut',
        xaxis_title='Année',
        yaxis_title='Nombre de Commandes',
        height=500
    )
    st.plotly_chart(fig4, use_container_width=True)

    with st.expander("Voir le tableau détaillé"):
        st.dataframe(df_annee_pivot.reset_index(), use_container_width=True)

# Footer
st.markdown("---")
st.caption("Dashboard Northwind ETL - Données extraites de SQL Server et Access")

