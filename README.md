# 🗄️ BMDB - Bouchettoy Marouan DataBase

**ORM Léger & Gestionnaire de Schémas pour le BM Framework**

[![Retour au Framework Principal](https://img.shields.io/badge/BM-Framework-black)](https://github.com/bm-framework)
[![PyPI Version](https://img.shields.io/pypi/v/bmdb)](https://pypi.org/project/bmdb/)

**BMDB** est le cœur de persistance des données du **BM Framework**. Il vous permet de définir vos modèles en YAML, de gérer les migrations de base de données et d'effectuer des opérations CRUD via un ORM simple ou une CLI puissante, **sans écrire une ligne de SQL**.

## ✨ Fonctionnalités

*   **🎯 Définition de modèles en YAML** : Déclarez vos tables et relations dans un fichier `models.bmdb` clair.
*   **🚀 Migrations automatiques** : Générez et exécutez les scripts SQL (ALTER TABLE, CREATE TABLE) en une commande.
*   **📦 ORM intuitif** : Opérations CRUD (`save()`, `get()`, `filter()`...) via Python.
*   **🛠️ CLI complète** : Gérez votre schéma de base de données entièrement depuis le terminal.
*   **🔌 Multi-bases** : Support natif de **PostgreSQL**, **MySQL** et **SQLite**.

## 📦 Installation

```bash
pip install bmdb
🚀 Utilisation en 30 Secondes
Créez un modèle :

bash
bmdb create-model Product name:String price:Float category:String
Générez et exécutez la migration :

bash
bmdb migrate-schema
Cette commande crée la table products dans votre base.

Utilisez l'ORM en Python :

python
from bmdb import Product

# Créer
new_product = Product(name="Ordinateur", price=999.99, category="Tech")
new_product.save()

# Lire
products = Product.filter(category="Tech")
for p in products:
    print(p.name, p.price)
🛠️ Référence de la CLI
Commande	Alias	Description
bmdb create-model <name> <fields...>    Crée un nouveau modèle avec ses champs (ex: title:String).
bmdb add-fields <model> <fields...> Ajoute des champs à un modèle existant.
bmdb migrate-schema Génère et exécute les migrations SQL pour synchroniser la BDD.
bmdb status	bmdb s	Affiche l'état des migrations (appliquées/en attente).
bmdb seed		Remplit la base avec des données de test définies dans seed.yml.
bmdb init		Initialise la configuration BMDB dans le projet courant.
📖 Référence de l'ORM (Méthodes Principales)
Méthode	Exemple	Description
.save()	product.save()	Crée ou met à jour l'enregistrement dans la base.
.delete()	product.delete()	Supprime l'enregistrement de la base.
.get(id)	Product.get(5)	Récupère un seul enregistrement par son ID.
.all()	Product.all()	Récupère tous les enregistrements de la table.
.filter(**kwargs)	Product.filter(category="Tech", price__gt=500)	Filtre les enregistrements (supporte __gt, __lt, etc.).
.first(**kwargs)	Product.first(name="Laptop")	Récupère le premier enregistrement correspondant.
.count()	Product.filter(category="Tech").count()	Compte le nombre d'enregistrements.
.to_dict()	product.to_dict()	Convertit l'objet en dictionnaire Python.
⚙️ Configuration
Créez un fichier .env à la racine de votre projet :

env
DB_CONNECTION="postgresql://user:password@localhost:5432/madb"
# ou pour SQLite : DB_CONNECTION="sqlite:///./database.db"
Définissez vos modèles dans models.bmdb (généré automatiquement par la CLI).

🔗 Faire partie d'une application complète
BMDB est conçu pour fonctionner de manière autonome OU comme fondation des autres modules du BM Framework :

Utilisez BMB pour exposer automatiquement vos modèles BMDB via une API RESTful.

Utilisez BMF pour générer des interfaces React qui interagissent avec cette API.

➡️ Découvrir le BM Framework complet

📄 Licence
MIT © Marouan Bouchettoy