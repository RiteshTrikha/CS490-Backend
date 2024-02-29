from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import mysql.connector
import json

app = Flask(__name__)
CORS(app)


# Database connection
def get_db_connection():
    conn = mysql.connector.connect(user='root', password='password', host='localhost', database='sakila')
    return conn


@app.route('/')
def index():
    return render_template('index.html')
@app.route('/api/top-movies')
def top_movies_all_stores():
    return top_movies()


@app.route('/api/store/<int:store_id>/top-movies')
def top_movies(store_id=None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if store_id:
        cursor.execute("""
            SELECT ft.film_id, ft.title, COUNT(r.rental_id) AS rented
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film_text ft ON i.film_id = ft.film_id
            JOIN store s ON i.store_id = s.store_id
            WHERE s.store_id = %s
            GROUP BY ft.film_id, ft.title
            ORDER BY rented DESC
            LIMIT 5;
        """, (store_id,))
    else:
        cursor.execute("""
            SELECT ft.film_id, ft.title, COUNT(r.rental_id) AS rented
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film_text ft ON i.film_id = ft.film_id
            GROUP BY ft.film_id, ft.title
            ORDER BY rented DESC
            LIMIT 5;
        """)

    movies = cursor.fetchall()
    conn.close()
    return jsonify(movies)


@app.route('/api/movie/<int:movie_id>')
def movie_details(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT title, description, release_year, rental_rate, length
        FROM film
        WHERE film_id = %s
    """, (movie_id,))
    movie = cursor.fetchone()
    conn.close()
    return jsonify(movie)


@app.route('/api/top-actors')
def top_actors():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT a.actor_id, a.first_name, a.last_name, COUNT(fa.film_id) AS film_count
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        GROUP BY a.actor_id
        ORDER BY film_count DESC
        LIMIT 5
    """)
    actors = cursor.fetchall()
    conn.close()
    return jsonify(actors)



@app.route('/api/actor/<int:actor_id>/top-films')
def actor_top_films(actor_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ft.film_id, ft.title, COUNT(r.rental_id) AS rental_count
        FROM film_actor fa
        JOIN actor a ON fa.actor_id = a.actor_id
        JOIN film f ON fa.film_id = f.film_id
        JOIN film_text ft ON f.film_id = ft.film_id
        JOIN inventory i ON f.film_id = i.film_id
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        WHERE a.actor_id = %s
        GROUP BY ft.film_id, ft.title
        ORDER BY rental_count DESC, ft.title ASC
        LIMIT 5
    """, (actor_id,))
    films = cursor.fetchall()
    conn.close()
    return jsonify(films)

@app.route('/films')
def films():
    return render_template('films.html')


# API endpoint for searching films
@app.route('/api/search-films', methods=['GET'])
def search_films():
    query = request.args.get('query')
    limit = request.args.get('limit', default=10, type=int)
    offset = request.args.get('offset', default=0, type=int)
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT 
            film.film_id, 
            film.title, 
            GROUP_CONCAT(DISTINCT actor.first_name, ' ', actor.last_name SEPARATOR ', ') AS actors,
            GROUP_CONCAT(DISTINCT category.name SEPARATOR ', ') AS genre
        FROM film
        LEFT JOIN film_actor ON film.film_id = film_actor.film_id
        LEFT JOIN actor ON film_actor.actor_id = actor.actor_id
        LEFT JOIN film_category ON film.film_id = film_category.film_id
        LEFT JOIN category ON film_category.category_id = category.category_id
        WHERE film.title LIKE %s 
        OR actor.first_name LIKE %s
        OR actor.last_name LIKE %s
        OR category.name LIKE %s
        OR film.film_id LIKE %s
        GROUP BY film.film_id
        LIMIT %s OFFSET %s
    """, ('%' + query + '%', '%' + query + '%', '%' + query + '%', '%' + query + '%', '%' + query + '%', limit, offset))
    films = cursor.fetchall()

    # Fetch total count of films matching the search criteria
    cursor.execute("""
        SELECT COUNT(DISTINCT film.film_id) AS total_count
        FROM film
        LEFT JOIN film_actor ON film.film_id = film_actor.film_id
        LEFT JOIN actor ON film_actor.actor_id = actor.actor_id
        LEFT JOIN film_category ON film.film_id = film_category.film_id
        LEFT JOIN category ON film_category.category_id = category.category_id
        WHERE film.title LIKE %s 
        OR actor.first_name LIKE %s
        OR actor.last_name LIKE %s
        OR category.name LIKE %s
        OR film.film_id LIKE %s
    """, ('%' + query + '%', '%' + query + '%', '%' + query + '%', '%' + query + '%', '%' + query + '%'))
    total_count = cursor.fetchone()['total_count']

    conn.close()
    return jsonify({'films': films, 'total_count': total_count})



@app.route('/api/film/<int:film_id>', methods=['GET'])
def film_details(film_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT film.title, film.description, film.release_year, film.rental_duration, film.rental_rate, film.length
        FROM film
        WHERE film.film_id = %s
    """, (film_id,))
    film = cursor.fetchone()
    conn.close()
    return jsonify(film)


@app.route('/customers')
def customers():
    return render_template('customers.html')


from flask import request, jsonify

@app.route('/api/customers', methods=['GET'])
def list_customers():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Pagination parameters
    limit = request.args.get('limit', default=10, type=int)
    offset = request.args.get('offset', default=0, type=int)

    # Fetch customers for the current page with all details including address
    cursor.execute("""
        SELECT 
            c.customer_id, 
            c.first_name, 
            c.last_name, 
            c.email,
            a.address,
            a.district,
            city.city,
            country.country,
            a.postal_code
        FROM 
            customer c
        INNER JOIN
            address a ON c.address_id = a.address_id
        INNER JOIN
            city ON a.city_id = city.city_id
        INNER JOIN
            country ON city.country_id = country.country_id
        ORDER BY
            c.customer_id
        LIMIT %s OFFSET %s

    """, (limit, offset))
    customers = cursor.fetchall()

    # Fetch total count of customers
    cursor.execute("SELECT COUNT(*) as total FROM customer")
    total_customers = cursor.fetchone()['total']

    # Calculate total pages
    total_pages = max(1, (total_customers + limit - 1) // limit)

    conn.close()

    return jsonify({'customers': customers, 'totalPages': total_pages})



if __name__ == '__main__':
    app.run(debug=True)
