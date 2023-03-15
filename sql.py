#Список комнат и количество студентов в каждой из них
rooms_number_of_students = "SELECT rooms.""name"" AS room_name, count(students.id) AS number_of_student \
						   	FROM rooms \
							JOIN students \
							ON rooms.id = students.room \
							GROUP BY room_name \
							ORDER BY room_name"

#5 комнат, где самый маленький средний возраст студентов
rooms_avg_age = "SELECT r.""name"" AS room_name, avg(age(current_date, s.birthday::date)) AS avg_age\
					FROM rooms r\
					JOIN students s\
					ON r.id = s.room\
					GROUP BY r.""name""\
					ORDER BY avg_age\
					LIMIT 5"

#5 комнат с самой большой разницей в возрасте студентов
rooms_diff_age = "SELECT DISTINCT r.""name"" AS room_name,\
					max(s.birthday) OVER(PARTITION BY r.""name"") - min(s.birthday) OVER(PARTITION BY r.""name"") AS age_diff\
					FROM rooms r\
					JOIN students s\
					ON r.id = s.room\
					ORDER BY age_diff DESC\
					LIMIT 5"

#Список комнат где живут разнополые студенты
rooms_diff_sex = "SELECT r.""name"" AS room_name\
					FROM rooms r\
					JOIN students s\
					ON r.id = s.room\
					WHERE upper(s.sex) = 'F'\
					INTERSECT\
					SELECT r.""name"" AS room_name\
					FROM rooms r\
					JOIN students s\
					ON r.id = s.room\
					WHERE upper(s.sex) = 'M'\
					ORDER BY room_name"