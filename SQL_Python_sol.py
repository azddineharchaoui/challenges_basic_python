from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, 
    Numeric, DateTime, Text, ForeignKey, insert, select, func, delete
)
from datetime import datetime
import logging
import os
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RestaurantDatabase:
    def __init__(self, connection_string):
        try:
            self.engine = create_engine(
                connection_string, 
                echo=False,
                pool_pre_ping=True,
                pool_recycle=300,
                connect_args={
                    "connect_timeout": 10,
                    "application_name": "restaurant_management_system"
                }
            )
            self.metadata = MetaData()
            self.define_tables()
            
            with self.engine.connect() as conn:
                conn.execute(select(1))
                logger.info("Connexion de base de donnée reussi!")
                
        except Exception as e:
            logger.error(f"Échec pour connecter a la database: {e}")
            raise
    
    def define_tables(self):
        self.categories = Table('categories', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('nom', String(50), nullable=False)
        )
        
        self.fournisseurs = Table('fournisseurs', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('nom', String(100), nullable=False),
            Column('contact', String(100), nullable=False)
        )
        
        self.plats = Table('plats', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('nom', String(100), nullable=False),
            Column('prix', Numeric(10, 2), nullable=False),
            Column('description', String(255)),
            Column('categorie_id', Integer, ForeignKey('categories.id'), nullable=False)
        )
        
        self.clients = Table('clients', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('nom', String(100), nullable=False),
            Column('email', String(100), nullable=False, unique=True),
            Column('telephone', String(20), nullable=True)
        )
        
        self.commandes = Table('commandes', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
            Column('date_commande', DateTime, nullable=False),
            Column('total', Numeric(10, 2), nullable=False)
        )
        
        self.ingredients = Table('ingredients', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('nom', String(100), nullable=False),
            Column('cout_unitaire', Numeric(10, 2), nullable=False),
            Column('stock', Numeric(10, 3), nullable=False),
            Column('fournisseur_id', Integer, ForeignKey('fournisseurs.id'), nullable=False)
        )
        
        self.commande_plats = Table('commande_plats', self.metadata,
            Column('commande_id', Integer, ForeignKey('commandes.id'), primary_key=True),
            Column('plat_id', Integer, ForeignKey('plats.id'), primary_key=True),
            Column('quantite', Integer, nullable=False, default=1)
        )
        
        self.plat_ingredients = Table('plat_ingredients', self.metadata,
            Column('plat_id', Integer, ForeignKey('plats.id'), primary_key=True),
            Column('ingredient_id', Integer, ForeignKey('ingredients.id'), primary_key=True),
            Column('quantite_necessaire', Numeric(10, 3), nullable=False)
        )
        
        self.avis = Table('avis', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
            Column('plat_id', Integer, ForeignKey('plats.id'), nullable=False),
            Column('note', Integer, nullable=False),
            Column('commentaire', Text, nullable=True),
            Column('date_avis', DateTime, nullable=False)
        )
    
    def create_tables(self):
        try:
            self.metadata.create_all(self.engine)
            logger.info("Tables cree avec succes")
        except Exception as e:
            logger.error(f"Erreur pendant la creation des tables: {e}")
            raise
    
    def drop_tables(self):
        try:
            self.metadata.drop_all(self.engine)
            logger.info("Tables supprimé")
        except Exception as e:
            logger.error(f"Erreur pendant suppression des tables: {e}")
            raise
    
    def insert_sample_data(self):
        with self.engine.connect() as conn:
            try:
                trans = conn.begin()
                
                categories_data = [
                    {'id': 1, 'nom': 'Entrée'},
                    {'id': 2, 'nom': 'Plat principal'},
                    {'id': 3, 'nom': 'Dessert'},
                    {'id': 4, 'nom': 'Boisson'},
                    {'id': 5, 'nom': 'Végétarien'}
                ]
                conn.execute(insert(self.categories), categories_data)
                
                fournisseurs_data = [
                    {'id': 1, 'nom': 'AgriFresh', 'contact': 'contact@agrifresh.com'},
                    {'id': 2, 'nom': 'MeatSupplier', 'contact': 'info@meatsupplier.com'},
                    {'id': 3, 'nom': 'BevCo', 'contact': 'sales@bevco.com'},
                    {'id': 4, 'nom': 'DairyFarm', 'contact': 'dairy@farm.com'}
                ]
                conn.execute(insert(self.fournisseurs), fournisseurs_data)
                
                plats_data = [
                    {'id': 1, 'nom': 'Salade César', 'prix': 45.00, 'description': 'Salade avec poulet grillé', 'categorie_id': 1},
                    {'id': 2, 'nom': 'Soupe de légumes', 'prix': 30.00, 'description': 'Soupe chaude de saison', 'categorie_id': 1},
                    {'id': 3, 'nom': 'Steak frites', 'prix': 90.00, 'description': 'Viande grillée et frites', 'categorie_id': 2},
                    {'id': 4, 'nom': 'Pizza Margherita', 'prix': 70.00, 'description': 'Pizza tomate & mozzarella', 'categorie_id': 2},
                    {'id': 5, 'nom': 'Tiramisu', 'prix': 35.00, 'description': 'Dessert italien', 'categorie_id': 3},
                    {'id': 6, 'nom': 'Glace 2 boules', 'prix': 25.00, 'description': 'Glace au choix', 'categorie_id': 3},
                    {'id': 7, 'nom': 'Coca-Cola', 'prix': 15.00, 'description': 'Boisson gazeuse', 'categorie_id': 4},
                    {'id': 8, 'nom': 'Eau minérale', 'prix': 10.00, 'description': 'Eau plate ou gazeuse', 'categorie_id': 4},
                    {'id': 9, 'nom': 'Curry de légumes', 'prix': 65.00, 'description': 'Plat végétarien épicé', 'categorie_id': 5},
                    {'id': 10, 'nom': 'Falafel wrap', 'prix': 50.00, 'description': 'Wrap avec falafels et légumes', 'categorie_id': 5}
                ]
                conn.execute(insert(self.plats), plats_data)
                
                clients_data = [
                    {'id': 1, 'nom': 'Amine Lahmidi', 'email': 'amine@example.com', 'telephone': '+212600123456'},
                    {'id': 2, 'nom': 'Sara Benali', 'email': 'sara.b@example.com', 'telephone': '+212600654321'},
                    {'id': 3, 'nom': 'Youssef El Khalfi', 'email': 'youssef.k@example.com', 'telephone': None},
                    {'id': 4, 'nom': 'Fatima Zahra', 'email': 'fatima.z@example.com', 'telephone': '+212600987654'},
                    {'id': 5, 'nom': 'Omar Alaoui', 'email': 'omar.a@example.com', 'telephone': '+212600112233'}
                ]
                conn.execute(insert(self.clients), clients_data)
                
                commandes_data = [
                    {'id': 1, 'client_id': 1, 'date_commande': datetime(2025, 7, 7, 12, 30), 'total': 120.00},
                    {'id': 2, 'client_id': 2, 'date_commande': datetime(2025, 7, 7, 13, 0), 'total': 85.00},
                    {'id': 3, 'client_id': 1, 'date_commande': datetime(2025, 7, 8, 19, 45), 'total': 150.00},
                    {'id': 4, 'client_id': 3, 'date_commande': datetime(2025, 8, 15, 18, 30), 'total': 200.00},
                    {'id': 5, 'client_id': 4, 'date_commande': datetime(2025, 9, 1, 20, 0), 'total': 95.00},
                    {'id': 6, 'client_id': 5, 'date_commande': datetime(2025, 9, 10, 12, 15), 'total': 75.00}
                ]
                conn.execute(insert(self.commandes), commandes_data)
                
                ingredients_data = [
                    {'id': 1, 'nom': 'Poulet', 'cout_unitaire': 15.00, 'stock': 50, 'fournisseur_id': 2},
                    {'id': 2, 'nom': 'Laitue', 'cout_unitaire': 5.00, 'stock': 20, 'fournisseur_id': 1},
                    {'id': 3, 'nom': 'Tomate', 'cout_unitaire': 3.00, 'stock': 30, 'fournisseur_id': 1},
                    {'id': 4, 'nom': 'Mozzarella', 'cout_unitaire': 10.00, 'stock': 15, 'fournisseur_id': 4},
                    {'id': 5, 'nom': 'Pomme de terre', 'cout_unitaire': 2.00, 'stock': 100, 'fournisseur_id': 1},
                    {'id': 6, 'nom': 'Café', 'cout_unitaire': 20.00, 'stock': 5, 'fournisseur_id': 3},
                    {'id': 7, 'nom': 'Sucre', 'cout_unitaire': 1.50, 'stock': 25, 'fournisseur_id': 3},
                    {'id': 8, 'nom': 'Pois chiches', 'cout_unitaire': 4.00, 'stock': 40, 'fournisseur_id': 1}
                ]
                conn.execute(insert(self.ingredients), ingredients_data)
                
                commande_plats_data = [
                    {'commande_id': 1, 'plat_id': 1, 'quantite': 1},
                    {'commande_id': 1, 'plat_id': 3, 'quantite': 1},
                    {'commande_id': 1, 'plat_id': 7, 'quantite': 2},
                    {'commande_id': 2, 'plat_id': 2, 'quantite': 1},
                    {'commande_id': 2, 'plat_id': 4, 'quantite': 1},
                    {'commande_id': 2, 'plat_id': 8, 'quantite': 1},
                    {'commande_id': 3, 'plat_id': 3, 'quantite': 1},
                    {'commande_id': 3, 'plat_id': 5, 'quantite': 1},
                    {'commande_id': 3, 'plat_id': 7, 'quantite': 1},
                    {'commande_id': 4, 'plat_id': 4, 'quantite': 2},
                    {'commande_id': 4, 'plat_id': 9, 'quantite': 1},
                    {'commande_id': 5, 'plat_id': 10, 'quantite': 1},
                    {'commande_id': 5, 'plat_id': 8, 'quantite': 2},
                    {'commande_id': 6, 'plat_id': 7, 'quantite': 3},
                    {'commande_id': 6, 'plat_id': 6, 'quantite': 1}
                ]
                conn.execute(insert(self.commande_plats), commande_plats_data)
                
                plat_ingredients_data = [
                    {'plat_id': 1, 'ingredient_id': 1, 'quantite_necessaire': 0.2},
                    {'plat_id': 1, 'ingredient_id': 2, 'quantite_necessaire': 0.1},
                    {'plat_id': 2, 'ingredient_id': 2, 'quantite_necessaire': 0.05},
                    {'plat_id': 2, 'ingredient_id': 5, 'quantite_necessaire': 0.1},
                    {'plat_id': 3, 'ingredient_id': 1, 'quantite_necessaire': 0.3},
                    {'plat_id': 3, 'ingredient_id': 5, 'quantite_necessaire': 0.2},
                    {'plat_id': 4, 'ingredient_id': 3, 'quantite_necessaire': 0.1},
                    {'plat_id': 4, 'ingredient_id': 4, 'quantite_necessaire': 0.15},
                    {'plat_id': 5, 'ingredient_id': 6, 'quantite_necessaire': 0.05},
                    {'plat_id': 5, 'ingredient_id': 7, 'quantite_necessaire': 0.02},
                    {'plat_id': 9, 'ingredient_id': 8, 'quantite_necessaire': 0.1},
                    {'plat_id': 10, 'ingredient_id': 8, 'quantite_necessaire': 0.15}
                ]
                conn.execute(insert(self.plat_ingredients), plat_ingredients_data)
                
                avis_data = [
                    {'id': 1, 'client_id': 1, 'plat_id': 1, 'note': 4, 'commentaire': 'Très frais, poulet bien cuit', 'date_avis': datetime(2025, 7, 7, 13, 0)},
                    {'id': 2, 'client_id': 2, 'plat_id': 4, 'note': 5, 'commentaire': 'Meilleure pizza du coin !', 'date_avis': datetime(2025, 7, 7, 14, 0)},
                    {'id': 3, 'client_id': 3, 'plat_id': 9, 'note': 3, 'commentaire': 'Un peu trop épicé', 'date_avis': datetime(2025, 8, 15, 19, 0)},
                    {'id': 4, 'client_id': 4, 'plat_id': 10, 'note': 4, 'commentaire': 'Bon, mais manque de sauce', 'date_avis': datetime(2025, 9, 1, 21, 0)},
                    {'id': 5, 'client_id': 5, 'plat_id': 6, 'note': 5, 'commentaire': 'Glace délicieuse', 'date_avis': datetime(2025, 9, 10, 13, 0)}
                ]
                conn.execute(insert(self.avis), avis_data)
                
                trans.commit()
                logger.info("Donnée d'exemple inseré avec succes")
                
            except Exception as e:
                trans.rollback()
                logger.error(f"Erreur pendant insertion des donnée: {e}")
                raise
    
    def execute_sample_queries(self):
        with self.engine.connect() as conn:
            try:
                print("\nRESULTATS DES REQUETE")
                print("=" * 50)
                
                print("\nTout les plats avec leur categorie:")
                print("-" * 40)
                query = select(
                    self.plats.c.nom.label('plat'),
                    self.plats.c.prix,
                    self.categories.c.nom.label('categorie')
                ).select_from(
                    self.plats.join(self.categories)
                ).order_by(self.categories.c.nom, self.plats.c.prix)
                
                result = conn.execute(query)
                for row in result:
                    print(f"{row.plat:<20} | {row.prix:>6.2f} DH | {row.categorie}")
                
                print("\nCommande d'Amine Lahmidi:")
                print("-" * 40)
                query = select(
                    self.commandes.c.id.label('commande_id'),
                    self.commandes.c.date_commande,
                    self.commandes.c.total
                ).select_from(
                    self.commandes.join(self.clients)
                ).where(
                    self.clients.c.nom == 'Amine Lahmidi'
                ).order_by(self.commandes.c.date_commande)
                
                result = conn.execute(query)
                for row in result:
                    print(f"Commande #{row.commande_id} | {row.date_commande} | {row.total} DH")
                
                print("\nTop 5 des plat les plus commandé:")
                print("-" * 40)
                query = select(
                    self.plats.c.nom.label('plat'),
                    func.sum(self.commande_plats.c.quantite).label('total_commande')
                ).select_from(
                    self.plats.join(self.commande_plats)
                ).group_by(
                    self.plats.c.id, self.plats.c.nom
                ).order_by(
                    func.sum(self.commande_plats.c.quantite).desc()
                ).limit(5)
                
                result = conn.execute(query)
                for i, row in enumerate(result, 1):
                    print(f"{i}. {row.plat:<20} | {row.total_commande} fois commandé")
                
                print("\nChiffre d'affaire par categorie:")
                print("-" * 40)
                query = select(
                    self.categories.c.nom.label('categorie'),
                    func.sum(self.plats.c.prix * self.commande_plats.c.quantite).label('ca')
                ).select_from(
                    self.categories
                    .join(self.plats)
                    .join(self.commande_plats)
                ).group_by(
                    self.categories.c.id, self.categories.c.nom
                ).order_by(
                    func.sum(self.plats.c.prix * self.commande_plats.c.quantite).desc()
                )
                
                result = conn.execute(query)
                for row in result:
                    print(f"{row.categorie:<15} | {row.ca:>8.2f} DH")
                
                print("\nClient les plus fidele:")
                print("-" * 40)
                query = select(
                    self.clients.c.nom.label('client'),
                    func.count(self.commandes.c.id).label('nb_commandes'),
                    func.sum(self.commandes.c.total).label('total_depense')
                ).select_from(
                    self.clients.join(self.commandes)
                ).group_by(
                    self.clients.c.id, self.clients.c.nom
                ).order_by(
                    func.count(self.commandes.c.id).desc()
                )
                
                result = conn.execute(query)
                for row in result:
                    print(f"{row.client:<20} | {row.nb_commandes} commandes | {row.total_depense:.2f} DH")
                
                print("\nNote moyenne des plats (avec avis):")
                print("-" * 40)
                query = select(
                    self.plats.c.nom.label('plat'),
                    func.avg(self.avis.c.note).label('note_moyenne'),
                    func.count(self.avis.c.id).label('nb_avis')
                ).select_from(
                    self.plats.join(self.avis)
                ).group_by(
                    self.plats.c.id, self.plats.c.nom
                ).order_by(
                    func.avg(self.avis.c.note).desc()
                )
                
                result = conn.execute(query)
                for row in result:
                    print(f"{row.plat:<20} | {row.note_moyenne:.1f}/5 | ({row.nb_avis} avis)")
                
            except Exception as e:
                logger.error(f"Erreur pendant execution des requete: {e}")
                raise


def get_connection_string():
    """Try different connection configurations"""
    
    if all(os.getenv(var) for var in ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']):
        return f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    
    local_configs = [
        'postgresql://postgres:root@localhost:5432/restaurant_db',
        'postgresql://postgres:postgres@localhost:5432/restaurant_db',
        'postgresql://restaurant_user:restaurant_pass@localhost:5432/restaurant_db',
    ]
    
    docker_configs = [
        'postgresql://postgres:root@postgres:5432/restaurant_db',
        'postgresql://postgres:postgres@postgres:5432/restaurant_db',
        'postgresql://postgres:root@db:5432/restaurant_db',
        'postgresql://postgres:postgres@db:5432/restaurant_db',
        'postgresql://postgres:root@127.0.0.1:5432/restaurant_db',
        'postgresql://postgres:postgres@127.0.0.1:5432/restaurant_db',
    ]
    
    all_configs = local_configs + docker_configs
    
    for config in all_configs:
        try:
            test_engine = create_engine(config, pool_pre_ping=True)
            with test_engine.connect() as conn:
                conn.execute(select(1))
                logger.info(f"Connexion reussi avec: {config.split('@')[0]}@{config.split('@')[1].split('/')[0]}/...")
                return config
        except Exception as e:
            logger.debug(f"Echec avec {config.split('@')[1].split('/')[0]}: {e}")
            continue
    
    return None


def create_database_if_not_exists(connection_string):
    try:
        base_conn_string = connection_string.rsplit('/', 1)[0] + '/postgres'
        engine = create_engine(base_conn_string)
        
        with engine.connect() as conn:
            conn.execute("COMMIT")
            
            db_name = connection_string.rsplit('/', 1)[1]
            result = conn.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
            
            if not result.fetchone():
                conn.execute(f"CREATE DATABASE {db_name}")
                logger.info(f"Base de donnée '{db_name}' cree")
            else:
                logger.info(f"Base de donnée '{db_name}' existe deja")
                
    except Exception as e:
        logger.warning(f"Impossible de creer la base de donnée: {e}")


def main():
    print("SYSTEME DE GESTION DE RESTAURANT")
    print("=" * 50)
    
    connection_string = get_connection_string()
    
    if not connection_string:
        print("\n❌ ERREUR: Impossible de se connecté a PostgreSQL")
        print("\nVerification a effectuer:")
        print("1. PostgreSQL est-il installé et demarré ?")
        print("2. Les parametre de connexion sont-ils correct ?")
        print("3. La base de donnée existe-t-elle ?")
        print("\nConfiguration testé:")
        print("• localhost:5432 (installation locale)")
        print("• postgres:5432 (conteneur Docker)")
        print("• db:5432 (conteneur Docker)")
        print("• 127.0.0.1:5432 (IP locale)")
        print("\nPour configurer avec des variable d'environnement:")
        print("export DB_HOST=localhost")
        print("export DB_USER=postgres") 
        print("export DB_PASSWORD=your_password")
        print("export DB_NAME=restaurant_db")
        print("export DB_PORT=5432")
        return sys.exit(1)
    
    print(f"Connexion trouvé: {connection_string.split('@')[0]}@{connection_string.split('@')[1].split('/')[0]}/...")
    
    try:
        create_database_if_not_exists(connection_string)
        
        db = RestaurantDatabase(connection_string)
        
        print("\nCreation des tables...")
        db.create_tables()
        
        print("\nInsertion des données d'exemple...")
        db.insert_sample_data()
        
        print("\nExécution des requêtes d'exemple...")
        db.execute_sample_queries()
        
        print("\n" + "=" * 50)
        print("✅ PROGRAMME TERMINE AVEC SUCCES")
        print("=" * 50)
        
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        print(f"\n Erreur: {e}")
        print("\nActions suggérées:")
        print("1. Vérifier que PostgreSQL est démarré")
        print("2. Vérifier les identifiants de connexion")
        print("3. Vérifier que la base de données existe")
        print("4. Vérifier les permissions utilisateur")
        return sys.exit(1)


if __name__ == "__main__":
    main()