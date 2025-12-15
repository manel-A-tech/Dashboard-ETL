import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

# config de la page
st.set_page_config(
    page_title="Dashboard Northwind ETL",
    layout="wide"
)

# titre principal
st.title("Dashboard ETL - Commandes")
st.markdown("---")

# bouton de rafraîchissement en haut
if st.button("Rafraîchir les données", type="primary"):
    with st.spinner("Extraction en cours..."):
        df_raw = extract_data()

    if not df_raw.empty:
        with st.spinner("Transformation en cours..."):
            df_transformed = transform_data(df_raw)

        with st.spinner("Chargement en cours..."):
            success = load_data(df_transformed)

        if success:
            st.session_state['data'] = df_transformed
            # Récupérer la dimension Date si elle existe
            if 'dim_date' in df_transformed.attrs:
                st.session_state['dim_date'] = df_transformed.attrs['dim_date']
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
            # Récupérer la dimension Date
            if 'dim_date' in df_transformed.attrs:
                st.session_state['dim_date'] = df_transformed.attrs['dim_date']

# vérification des données
if 'data' not in st.session_state or st.session_state['data'].empty:
    #st.warning("Aucune donnée disponible. Cliquez sur "Rafraîchir les données".")
    st.stop()

df = st.session_state['data']
dim_date = st.session_state.get('dim_date', None)

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
tab1, tab2, tab3 = st.tabs(["Par Date", "Par Client", "Par Employé"])

# ------------------ DATE ------------------
with tab1:
    st.subheader("Évolution Temporelle Complète")
    
    if dim_date is not None and not dim_date.empty:
        # Statistiques de la dimension
        col_a, col_b, col_c, col_d = st.columns(4)
        
        with col_a:
            st.metric("Période totale", f"{len(dim_date):,} jours")
        
        with col_b:
            st.metric("Début", dim_date['Date'].min().strftime('%d/%m/%Y'))
        
        with col_c:
            st.metric("Fin", dim_date['Date'].max().strftime('%d/%m/%Y'))
        
        with col_d:
            nb_annees = dim_date['Annee'].nunique()
            st.metric("Années", f"{nb_annees}")
        
        st.markdown("---")
        
        # Préparer les données : compter les commandes par date et par statut
        df_temp = df_filtered.copy()
        df_temp['Date_Only'] = df_temp['OrderDate'].dt.date
        df_par_jour_statut = df_temp.groupby(['Date_Only', 'Status_Livraison']).size().reset_index(name='Commandes')
        df_par_jour_statut.columns = ['Date', 'Status_Livraison', 'Commandes']
        
        # Créer les séries pour Livrée et Non Livrée
        df_livree = df_par_jour_statut[df_par_jour_statut['Status_Livraison'] == 'Livrée'][['Date', 'Commandes']]
        df_non_livree = df_par_jour_statut[df_par_jour_statut['Status_Livraison'] == 'Non Livrée'][['Date', 'Commandes']]
        
        # Joindre avec la dimension Date pour avoir tous les jours
        dim_date_copy = dim_date.copy()
        dim_date_copy['Date'] = pd.to_datetime(dim_date_copy['Date']).dt.date
        
        df_complet_livree = dim_date_copy[['Date']].merge(df_livree, on='Date', how='left').fillna(0)
        df_complet_non_livree = dim_date_copy[['Date']].merge(df_non_livree, on='Date', how='left').fillna(0)
        
        # Graphique principal : Timeline complète avec deux courbes séparées
        fig_timeline = go.Figure()
        
        # Courbe pour les commandes livrées (style similaire à l'image)
        fig_timeline.add_trace(go.Scatter(
            x=df_complet_livree['Date'],
            y=df_complet_livree['Commandes'],
            mode='lines+markers',
            name='Livrée',
            line=dict(color='#00d4ff', width=2),
            marker=dict(size=4, color='#00d4ff'),
            hovertemplate='<b>Date:</b> %{x}<br><b>Livrée:</b> %{y}<extra></extra>'
        ))
        
        # Courbe pour les commandes non livrées (style similaire à l'image)
        fig_timeline.add_trace(go.Scatter(
            x=df_complet_non_livree['Date'],
            y=df_complet_non_livree['Commandes'],
            mode='lines+markers',
            name='Non Livrée',
            line=dict(color='#ff6b9d', width=2),
            marker=dict(size=4, color='#ff6b9d'),
            hovertemplate='<b>Date:</b> %{x}<br><b>Non Livrée:</b> %{y}<extra></extra>'
        ))
        
        fig_timeline.update_layout(
            title='Évolution Complète des Commandes par Date',
            xaxis_title='Mois',
            yaxis_title='Nombre de Commandes',
            height=500,
            hovermode='x unified',
            plot_bgcolor='#1a1a2e',
            paper_bgcolor='#1a1a2e',
            font=dict(color='white'),
            xaxis=dict(
                gridcolor='#2d2d44',
                showgrid=True
            ),
            yaxis=dict(
                gridcolor='#2d2d44',
                showgrid=True
            ),
            legend=dict(
                orientation="h",
                yanchor="top",
                y=1.1,
                xanchor="right",
                x=1,
                bgcolor='rgba(0,0,0,0)'
            )
        )
        
        st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Statistiques supplémentaires
        st.markdown("---")
        st.subheader("Statistiques de la période")
        
        col_stat1, col_stat2, col_stat3 = st.columns(3)
        
        with col_stat1:
            jours_avec_commandes = len(df_par_jour_statut['Date'].unique())
            jours_sans_commandes = len(dim_date) - jours_avec_commandes
            st.metric("Jours avec commandes", f"{jours_avec_commandes:,}")
            st.metric("Jours sans commandes", f"{jours_sans_commandes:,}")
        
        with col_stat2:
            df_temp_stats = df_filtered.copy()
            df_temp_stats['Date_Only'] = df_temp_stats['OrderDate'].dt.date
            commandes_par_jour_moy = df_temp_stats.groupby('Date_Only').size().mean()
            st.metric("Moyenne commandes/jour", f"{commandes_par_jour_moy:.1f}")
            max_commandes_jour = df_temp_stats.groupby('Date_Only').size().max()
            st.metric("Maximum commandes/jour", f"{max_commandes_jour:,}")
        
        with col_stat3:
            total_mois = dim_date['Mois_Annee'].nunique()
            st.metric("Nombre de mois", f"{total_mois}")
            total_trimestres = dim_date['Trimestre'].nunique() * dim_date['Annee'].nunique()
            st.metric("Nombre de trimestres", f"{total_trimestres}")
        
        # Tableau de la dimension Date
        with st.expander("Voir la table de dimension Date complète"):
            st.dataframe(
                dim_date.head(100),
                use_container_width=True,
                height=400
            )
            st.info(f"Affichage des 100 premières lignes sur {len(dim_date)} jours au total")
        
    else:
        st.warning(" Dimension Date non disponible. Rafraîchissez les données.")

# ------------------ CLIENTS ------------------
with tab2:
    st.subheader(" Analyse par Client")

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
with tab3:
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

# Footer
st.markdown("---")
st.caption(" Dashboard Northwind ETL - Données extraites de SQL Server et Access")