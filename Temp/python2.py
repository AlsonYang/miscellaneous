class SalaryError(ValueError): 
    pass
class Employee:
    def __init__(self, salary):
        self._salary = salary
    @property
    def salary(self):
        return self._salary
    @salary.setter
    def salary(self, new_salary):
        if new_salary < 0:
            raise SalaryError('Salary cannot go below 0')
        else:
            self._salary = new_salary
alson = Employee(10)
alson.salary = -100
print(alson.salary
)